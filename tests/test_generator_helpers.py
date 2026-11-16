"""Unit tests for the synthetic data generator's helper functions (notebook 01)."""
import numpy as np

from conftest import load_generator_helpers


def luhn_valid_npi(npi: str) -> bool:
    """Independent NPI check: Luhn over the '80840' prefix plus all 10 digits must sum to a multiple of 10."""
    total = 0
    for i, ch in enumerate(reversed("80840" + npi)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


# ---- NPI ---------------------------------------------------------------------

def test_npi_check_digit_matches_cms_example(generator):
    # CMS's published example: the NPI 1234567893 has check digit 3.
    assert generator["npi_check_digit"]("123456789") == "3"


def test_independent_luhn_check_agrees_with_cms_example():
    assert luhn_valid_npi("1234567893")
    assert not luhn_valid_npi("1234567894")


def test_generated_npis_are_valid_unique_and_well_formed(generator):
    npis = generator["make_npis"](2_000)
    assert len(set(npis)) == len(npis), "NPIs must be unique"
    for npi in npis:
        assert len(npi) == 10 and npi.isdigit() and npi.startswith("1")
        assert luhn_valid_npi(npi), npi


def test_npis_are_reproducible_for_the_same_seed():
    first = load_generator_helpers()["make_npis"](50)
    second = load_generator_helpers()["make_npis"](50)
    assert list(first) == list(second)


# ---- Dates and delivery labels ----------------------------------------------

def _days(*iso_dates):
    return np.array([np.datetime64(d, "D").astype(int) for d in iso_dates])


def test_days_to_str_round_trips(generator):
    out = generator["days_to_str"](_days("2024-01-01", "2025-12-31"))
    assert list(out) == ["2024-01-01", "2025-12-31"]


def test_delivery_label_is_the_fill_month(generator):
    labels = generator["delivery_label"](_days("2024-01-15", "2024-02-29", "2025-12-31"))
    assert list(labels) == ["2024_01", "2024_02", "2025_12"]


def test_rows_after_the_period_go_to_the_late_file(generator):
    labels = generator["delivery_label"](_days("2026-01-02", "2026-03-10"))
    assert list(labels) == [generator["LATE_DELIVERY"]] * 2


def test_delivery_label_regression_no_year_overflow(generator):
    # Regression: casting int days straight to months once produced labels like '3673_01'.
    labels = generator["delivery_label"](_days("2024-06-30"))
    assert labels[0] == "2024_06"


# ---- Configuration -------------------------------------------------------------

def test_ndc_format_mix_sums_to_one(generator):
    assert abs(sum(generator["NDC_FORMAT_MIX"].values()) - 1) < 1e-9


def test_messiness_rates_are_proportions(generator):
    rates = {k: v for k, v in generator.items() if k.startswith("RATE_")}
    assert rates, "expected RATE_* constants"
    for name, value in rates.items():
        assert 0 < value < 1, f"{name}={value}"


def test_period_is_two_full_years(generator):
    assert str(generator["PERIOD_START"]) == "2024-01-01"
    assert str(generator["PERIOD_END"]) == "2025-12-31"
