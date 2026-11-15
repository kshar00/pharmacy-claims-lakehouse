# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic pharmacy claims data
# MAGIC
# MAGIC Writes five synthetic source extracts as **CSV** into the landing volume, laid out like the files a PBM receives:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/portfolio/raw/landing/
# MAGIC   claims/            claims_delivery_2024_01.csv ... claims_delivery_2025_12.csv   (monthly deliveries)
# MAGIC                      claims_delivery_2026_01.csv                                    (late file: late claims + resends)
# MAGIC   drug_reference/    drug_reference.csv
# MAGIC   members/           members.csv
# MAGIC   pharmacies/        pharmacies.csv
# MAGIC   rebate_contracts/  rebate_contracts.csv
# MAGIC   _manifest/         generation_manifest.json   (answer key: exactly what messiness was injected)
# MAGIC ```
# MAGIC
# MAGIC **About 1.0 million claim rows** covering fill dates 2024-01-01 to 2025-12-31, ~50k members, ~2.5k pharmacies.
# MAGIC
# MAGIC ### Deliberate messiness (so silver has real work to do)
# MAGIC | Issue | How it shows up | Silver rule that handles it |
# MAGIC |---|---|---|
# MAGIC | Mixed NDC formats | 11-digit plain, 10-digit dashed (4-4-2 / 5-3-2 / 5-4-1), 11-digit dashed, leading zeros lost | Normalise to `ndc11` |
# MAGIC | Invalid NDCs | Junk/null values; valid-looking NDCs missing from `drug_reference` | Route to `silver.claims_rejected` with a reason code |
# MAGIC | Reversals | `claim_status = 'reversed'`, negative amounts, `reversal_of_claim_id` -> original. ~Half are re-billed | Net reversals |
# MAGIC | Orphan reversals | Reversal pointing at a claim that never arrived | DQ referential check |
# MAGIC | Duplicates | Exact double-writes in the same file; resends in the late file, some with a new `claim_id` or corrected copay | Dedupe on `(rx_number, fill_date, pharmacy_npi)`, latest ingestion wins |
# MAGIC | Eligibility gaps | Claims filled before a member's start or after their term date | Flag outside eligibility |
# MAGIC | Unknown keys | `member_id` / `pharmacy_npi` not in the reference files; null `prescriber_npi` | DQ null + referential checks |
# MAGIC | Late-arriving files | 3% of claims delivered only in the Jan-2026 file, so some reversals arrive *before* their original | Netting must not depend on arrival order |
# MAGIC
# MAGIC ### Design choices (see `docs/design_decisions.md`)
# MAGIC * **numpy + pandas on the driver, not Spark.** 1M rows is ~200 MB in memory and generates in well under a minute on serverless.
# MAGIC   Vectorised numpy is easier to read for row-level rules like "this reversal points at that claim", and one seeded RNG
# MAGIC   makes every run produce **identical files**. Spark would only be worth it at around 100M+ rows.
# MAGIC * **CSV, not Parquet.** Real PBM extracts are flat files, and CSV has no schema. That keeps real bronze problems in play:
# MAGIC   if you let Spark infer types, `ndc` becomes an integer and drops its leading zeros.
# MAGIC * **Every messiness rate is a named constant** below, and the actual injected counts go to `_manifest/` so silver
# MAGIC   can be checked against a known answer key.

# COMMAND ----------

# MAGIC %md ## Parameters
# MAGIC * `catalog` – change only if you could not create `portfolio` in notebook 00.
# MAGIC * `write_mode` –
# MAGIC   * `all` (default, used by the Job): wipe landing and write everything.
# MAGIC   * `initial_only`: wipe landing and write reference files + the 24 regular monthly deliveries, but **not** the late file.
# MAGIC   * `late_only`: write just the Jan-2026 late file. Run this after bronze has loaded `initial_only` to show incremental ingestion.
# MAGIC     Because the RNG is seeded, the late file matches the claims already loaded.
# MAGIC * `random_seed` – same seed gives byte-identical files.

# COMMAND ----------

dbutils.widgets.text("catalog", "portfolio", "Catalog")
dbutils.widgets.dropdown("write_mode", "all", ["all", "initial_only", "late_only"], "Write mode")
dbutils.widgets.text("random_seed", "42", "Random seed")

CATALOG = dbutils.widgets.get("catalog")
WRITE_MODE = dbutils.widgets.get("write_mode")
SEED = int(dbutils.widgets.get("random_seed"))

LANDING = f"/Volumes/{CATALOG}/raw/landing"
print(f"Landing path: {LANDING} | write_mode={WRITE_MODE} | seed={SEED}")

# COMMAND ----------

# MAGIC %md ## Configuration
# MAGIC All volumes and messiness rates live here so they are easy to explain and tune.

# COMMAND ----------

import json
import os

import numpy as np
import pandas as pd

rng = np.random.default_rng(SEED)

# ---- Time window -------------------------------------------------------------
PERIOD_START = np.datetime64("2024-01-01", "D")
PERIOD_END = np.datetime64("2025-12-31", "D")
LATE_DELIVERY = "2026_01"  # delivery label of the late file (arrives after the period closes)

# ---- Volumes -----------------------------------------------------------------
# ~920k original paid claims + reversals, rebills, duplicates gives ~1.0M rows on disk.
N_BASE_CLAIMS = 920_000
N_MEMBERS = 50_000  # about 9-10 scripts per member per year, close to a commercial book
N_PHARMACIES = 2_500
N_MAIL_PHARMACIES = 6  # a handful of large mail-order facilities
N_SPECIALTY_PHARMACIES = 150
N_PRESCRIBERS = 15_000

# ---- Row-level messiness (applied to disjoint sets of original claims) ------
RATE_OUTSIDE_ELIGIBILITY = 0.015  # fill_date before eligibility_start or after eligibility_end
RATE_UNKNOWN_MEMBER = 0.002  # member_id not in members.csv
RATE_UNKNOWN_PHARMACY = 0.002  # pharmacy_npi not in pharmacies.csv
RATE_NULL_PRESCRIBER = 0.005  # prescriber_npi missing

# ---- Claim lifecycle messiness -----------------------------------------------
RATE_REVERSAL = 0.04  # share of original claims later reversed
RATE_REBILL = 0.50  # share of reversed claims re-submitted as a corrected paid claim
RATE_EXACT_DUP = 0.010  # identical row written twice in the same file
RATE_RESEND_DUP = 0.015  # same claim re-sent in the late file
RATE_RESEND_NEW_ID = 0.50  # ...of which this share carry a *new* claim_id (only the business key catches them)
RATE_RESEND_CORRECTED = 0.30  # ...of which this share have a corrected copay (latest version must win)
RATE_LATE = 0.03  # original claims delivered only in the late file
N_ORPHAN_REVERSALS = 250  # reversals pointing at a claim_id that never existed

# ---- NDC format mix on original claims (must sum to 1) -----------------------
NDC_FORMAT_MIX = {
    "ndc11_plain": 0.600,  # 00093721401
    "ndc10_dashed": 0.250,  # 0093-7214-01 / 12345-678-90 / 12345-6789-0
    "ndc11_dashed": 0.050,  # 00093-7214-01
    "leading_zeros_lost": 0.085,  # 93721401  (Excel/integer cast stripped the zeros)
    "invalid_format": 0.008,  # null, 'UNKNOWN', wrong length, letters
    "not_in_reference": 0.007,  # well-formed NDC but missing from drug_reference (e.g. new launch)
}
assert abs(sum(NDC_FORMAT_MIX.values()) - 1) < 1e-9

MONEY_COLS = ["ingredient_cost", "dispensing_fee", "copay", "plan_paid"]
CLAIM_COLS = [
    "claim_id", "rx_number", "fill_date", "member_id", "pharmacy_npi", "prescriber_npi", "ndc",
    "quantity_dispensed", "days_supply", *MONEY_COLS, "claim_status", "reversal_of_claim_id",
]

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------


def npi_check_digit(base9: str) -> str:
    """CMS NPI check digit: Luhn over the prefix '80840' + the first 9 digits.

    Generating NPIs that pass the real checksum means a silver DQ rule can validate them.
    """
    total = 0
    for i, ch in enumerate(reversed("80840" + base9)):
        d = int(ch)
        if i % 2 == 0:  # double every other digit, starting with the right-most payload digit
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - total % 10) % 10)


def make_npis(n: int) -> np.ndarray:
    tails = rng.choice(10**8, size=n, replace=False)  # unique, so no two providers share an NPI
    return np.array([f"1{t:08d}" + npi_check_digit(f"1{t:08d}") for t in tails], dtype=object)


def days_to_str(day_numbers: np.ndarray) -> np.ndarray:
    """int days-since-epoch -> 'YYYY-MM-DD' strings."""
    return np.datetime_as_string(day_numbers.astype("datetime64[D]"), unit="D")


def delivery_label(day_numbers: np.ndarray) -> np.ndarray:
    """Delivery file label 'YYYY_MM' for the month a row arrives in; anything after the period goes in the late file."""
    months = day_numbers.astype("datetime64[D]").astype("datetime64[M]")  # ints must become days first, then months
    labels = np.char.replace(np.datetime_as_string(months, unit="M"), "-", "_")
    return np.where(day_numbers > PERIOD_END.astype(int), LATE_DELIVERY, labels).astype(object)


def write_csv(df: pd.DataFrame, relative_path: str) -> None:
    path = f"{LANDING}/{relative_path}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  wrote {len(df):>9,} rows -> {relative_path}")


P0, P1 = PERIOD_START.astype(int), PERIOD_END.astype(int)

# COMMAND ----------

# MAGIC %md ## 1. Drug reference
# MAGIC Fictional drug and manufacturer names on purpose: synthetic NDCs attached to real product names could read as real pricing.
# MAGIC Therapeutic classes and cost levels are realistic, so the numbers come out plausible:
# MAGIC specialty is about 2% of claims and roughly half of spend.
# MAGIC
# MAGIC **NDC construction mirrors the real rule:** an NDC is a 10-digit code in 4-4-2, 5-3-2 or 5-4-1 segments, and the 11-digit 5-4-2 form
# MAGIC pads the short segment with a zero. Each manufacturer has a labeler code. That means every `ndc11` has exactly one padding zero,
# MAGIC so silver can only convert a 10-digit NDC correctly if it uses the **dash positions** to know which segment to pad.

# COMMAND ----------

# fmt: off
THERAPEUTIC_CLASSES = [
    # class,                specialty, claim share, n drugs, brand share, qty per 30 days, UOM, form,      generic $/unit,  brand $/unit,   stem
    ("Antihypertensives",   "N", 0.170, 70, 0.10, 30, "EA", "TAB",     (0.05, 0.60),    (4, 12),        "pril"),
    ("Lipid Lowering",      "N", 0.130, 40, 0.10, 30, "EA", "TAB",     (0.05, 0.50),    (8, 20),        "statin"),
    ("Antidiabetics",       "N", 0.100, 50, 0.45, 60, "EA", "TAB",     (0.05, 0.40),    (6, 16),        "gliptin"),
    ("Antidepressants",     "N", 0.110, 45, 0.08, 30, "EA", "TAB",     (0.05, 0.60),    (8, 15),        "oxetine"),
    ("Analgesics",          "N", 0.070, 35, 0.05, 60, "EA", "TAB",     (0.03, 0.40),    (3, 8),         "profen"),
    ("Anti-infectives",     "N", 0.090, 50, 0.07, 60, "EA", "CAP",     (0.10, 1.50),    (5, 20),        "cillin"),
    ("Respiratory",         "N", 0.070, 35, 0.50, 1,  "EA", "INHALER", (20, 60),        (250, 400),     "terol"),
    ("Gastrointestinal",    "N", 0.070, 30, 0.10, 30, "EA", "CAP",     (0.05, 0.80),    (8, 15),        "prazole"),
    ("Thyroid",             "N", 0.060, 12, 0.10, 30, "EA", "TAB",     (0.05, 0.40),    (1, 3),         "thyrox"),
    ("Anticoagulants",      "N", 0.040, 15, 0.60, 60, "EA", "TAB",     (0.10, 0.80),    (7, 10),        "xaban"),
    ("Dermatology",         "N", 0.033, 30, 0.25, 30, "GM", "CREAM",   (0.10, 1.00),    (5, 15),        "sone"),
    ("Immunology",          "Y", 0.006, 18, 0.85, 2,  "ML", "INJ",     (1200, 1600),    (3000, 4000),   "mab"),
    ("Oncology",            "Y", 0.004, 25, 0.80, 30, "EA", "TAB",     (150, 250),      (400, 600),     "tinib"),
    ("Multiple Sclerosis",  "Y", 0.002, 10, 0.85, 1,  "ML", "INJ",     (2500, 3500),    (7000, 9000),   "mod"),
    ("HIV",                 "Y", 0.004, 15, 0.85, 30, "EA", "TAB",     (40, 60),        (100, 130),     "vir"),
    ("Hepatitis C",         "Y", 0.001, 5,  0.90, 28, "EA", "TAB",     (300, 400),      (850, 1000),    "buvir"),
]
# fmt: on

BRAND_MANUFACTURERS = [
    "Aldermoor Pharmaceuticals", "Brightwell Biologics", "Caldera Therapeutics", "Dunmore Labs",
    "Everline Pharma", "Fairhaven Bio", "Greystone Therapeutics", "Halcyon Biosciences",
    "Ironbridge Pharma", "Juniper Ridge Bio", "Kestrel Therapeutics", "Larchmont Pharma",
]
GENERIC_MANUFACTURERS = [
    "Meridian Generics", "Northgate Rx Labs", "Oakvale Pharmaceuticals", "Pinecrest Generics",
    "Quarry Lane Labs", "Riverbend Pharma", "Silverleaf Generics", "Tidewater Labs",
    "Upland Pharmaceuticals", "Valewood Generics",
]

# Each manufacturer gets one labeler code: 4-digit labelers use 4-4-2, 5-digit labelers use 5-3-2 or 5-4-1.
all_mfrs = BRAND_MANUFACTURERS + GENERIC_MANUFACTURERS
labeler_len = rng.choice([4, 5], size=len(all_mfrs), p=[0.4, 0.6])
labeler_code = {}
for m, L in zip(all_mfrs, labeler_len):
    while True:  # labeler codes must be unique or two manufacturers could share NDCs
        code = f"{rng.integers(1000, 10000):04d}" if L == 4 else f"{rng.integers(10000, 100000):05d}"
        if code not in labeler_code.values():
            labeler_code[m] = code
            break

SYLLABLES_A = ["Ab", "Bel", "Cor", "Dal", "Ex", "Fen", "Gal", "Hal", "Il", "Jor", "Kal", "Lum",
               "Mer", "Nov", "Or", "Pra", "Quel", "Ros", "Sal", "Tor", "Ul", "Ven", "Xan", "Zor"]
SYLLABLES_B = ["a", "e", "i", "o", "u", "ri", "lo", "ta", "ve", "xi", "na", "mi"]
BRAND_ENDINGS = ["ra", "vex", "lia", "tra", "zon", "qua", "nix", "via", "dra", "lyn"]
STRENGTHS = [2.5, 5, 10, 20, 25, 40, 50, 100, 150, 200]
PACKAGE_SIZES = {"EA": [30, 90, 500], "ML": [1, 2, 4], "GM": [15, 30, 60]}

used_names: set = set()
used_products: dict = {m: set() for m in all_mfrs}


def unique_name(stem: str, brand: bool) -> str:
    while True:
        a, b = rng.choice(SYLLABLES_A), rng.choice(SYLLABLES_B)
        name = f"{a}{b}{rng.choice(BRAND_ENDINGS)}" if brand else f"{a.lower()}{b}{stem}".capitalize()
        if name not in used_names:
            used_names.add(name)
            return name


def new_ndc(mfr: str, n_packages: int):
    """Return [(ndc11, ndc10_dashed), ...] for one product's packages, following the 10->11 padding rule."""
    lab = labeler_code[mfr]
    fmt = "4-4-2" if len(lab) == 4 else str(rng.choice(["5-3-2", "5-4-1"], p=[0.7, 0.3]))
    prod_len = 4 if fmt in ("4-4-2", "5-4-1") else 3
    while True:
        prod = f"{rng.integers(0, 10**prod_len):0{prod_len}d}"
        if prod not in used_products[mfr]:
            used_products[mfr].add(prod)
            break
    out = []
    for p in range(1, n_packages + 1):
        pkg = f"{p:01d}" if fmt == "5-4-1" else f"{p:02d}"
        ndc10_dashed = f"{lab}-{prod}-{pkg}"
        ndc11 = f"{lab:0>5}{prod:0>4}{pkg:0>2}"  # pad whichever segment is short
        out.append((ndc11, ndc10_dashed))
    return out


drug_rows = []
for (cls, spec, share, n_drugs, brand_share, qty30, uom, form, g_cost, b_cost, stem) in THERAPEUTIC_CLASSES:
    is_brand = rng.random(n_drugs) < max(brand_share, 0.15)
    is_brand[0], is_brand[-1] = True, False  # guarantee at least one brand and one generic per class
    # Zipf popularity within the class; brand drugs together take `brand_share` of the class's claims
    zipf = 1 / np.arange(1, n_drugs + 1) ** 1.1
    rng.shuffle(zipf)
    w = np.where(is_brand, zipf / zipf[is_brand].sum() * brand_share, zipf / zipf[~is_brand].sum() * (1 - brand_share))
    for i in range(n_drugs):
        brand = bool(is_brand[i])
        mfr = str(rng.choice(BRAND_MANUFACTURERS if brand else GENERIC_MANUFACTURERS))
        lo, hi = b_cost if brand else g_cost
        unit_cost = float(np.exp(rng.uniform(np.log(lo), np.log(hi))))  # log-uniform: a spread of cheap and pricey products
        name = f"{unique_name(stem, brand)} {rng.choice(STRENGTHS):g} MG {form}"
        n_pk = int(rng.integers(1, 4))
        for ndc11, ndc10_dashed in new_ndc(mfr, n_pk):
            drug_rows.append(dict(
                ndc11=ndc11, drug_name=name, brand_generic_flag="B" if brand else "G", specialty_flag=spec,
                therapeutic_class=cls, manufacturer=mfr,
                package_size=int(rng.choice(PACKAGE_SIZES[uom])), unit_of_measure=uom,
                # internal-only columns (not written to the reference file):
                _ndc10_dashed=ndc10_dashed, _unit_cost=unit_cost, _qty_per_30=qty30,
                _claim_weight=share * w[i] / n_pk,
            ))

drugs = pd.DataFrame(drug_rows)
assert drugs.ndc11.is_unique and drugs.ndc11.str.len().eq(11).all()
drugs["_claim_weight"] /= drugs["_claim_weight"].sum()
drugs["_ndc11_dashed"] = drugs.ndc11.str[:5] + "-" + drugs.ndc11.str[5:9] + "-" + drugs.ndc11.str[9:]
drugs["_ndc11_no_zeros"] = drugs.ndc11.str.lstrip("0")
print(f"{len(drugs):,} NDCs across {drugs.drug_name.nunique():,} drugs")

# COMMAND ----------

# MAGIC %md ## 2. Pharmacies and prescribers
# MAGIC NPIs pass the real CMS Luhn check, so a DQ rule can validate them in silver.

# COMMAND ----------

# Approximate population weights; the remaining states share the rest
STATE_WEIGHTS = {"CA": 12, "TX": 9, "FL": 7, "NY": 6, "PA": 4, "IL": 4, "OH": 3.5, "GA": 3.3, "NC": 3.2, "MI": 3,
                 "NJ": 2.8, "VA": 2.6, "WA": 2.3, "AZ": 2.2, "MA": 2.1, "TN": 2.1, "IN": 2, "MO": 1.9, "MD": 1.9, "WI": 1.8,
                 "CO": 1.8, "MN": 1.7, "SC": 1.6, "AL": 1.5, "LA": 1.4, "KY": 1.4, "OR": 1.3, "OK": 1.2, "CT": 1.1, "UT": 1}
STATES = np.array(list(STATE_WEIGHTS))
STATE_P = np.array(list(STATE_WEIGHTS.values())) / sum(STATE_WEIGHTS.values())

all_npis = make_npis(N_PHARMACIES + N_PRESCRIBERS)
pharmacy_npis, prescriber_npis = all_npis[:N_PHARMACIES], all_npis[N_PHARMACIES:]

ph_type = np.array(["mail"] * N_MAIL_PHARMACIES + ["specialty"] * N_SPECIALTY_PHARMACIES
                   + ["retail"] * (N_PHARMACIES - N_MAIL_PHARMACIES - N_SPECIALTY_PHARMACIES), dtype=object)
RETAIL_BANNERS = ["CornerCare Pharmacy", "HealthHub Drug", "Main Street Rx", "WellPoint Pharmacy", "Village Apothecary",
                  "FreshMart Pharmacy", "QuickScript Drug", "Neighborhood Rx"]
SPECIALTY_BANNERS = ["Apex Specialty Pharmacy", "Summit Specialty Rx", "Beacon Specialty Pharmacy", "Crescent Specialty Care"]
MAIL_BANNERS = ["Direct Home Delivery Pharmacy", "MailRx Fulfillment Center"]

ph_name = []
for i, t in enumerate(ph_type):
    banners = {"retail": RETAIL_BANNERS, "specialty": SPECIALTY_BANNERS, "mail": MAIL_BANNERS}[t]
    ph_name.append(f"{banners[i % len(banners)]} #{1000 + i}")

pharmacies = pd.DataFrame({
    "pharmacy_npi": pharmacy_npis,
    "name": ph_name,
    "type": ph_type,
    "state": rng.choice(STATES, size=N_PHARMACIES, p=STATE_P),
})
print(pharmacies.type.value_counts().to_dict())

# COMMAND ----------

# MAGIC %md ## 3. Members
# MAGIC Eligibility starts on the 1st of a month and terms at month-end, as real enrollment feeds do.
# MAGIC About 75% of members are still active, so their **`eligibility_end` is blank** (silver should read null as "open-ended").
# MAGIC About 45% enrol during the claim period, which gives the outside-eligibility injection real gaps to fall into.

# COMMAND ----------

PLANS = np.array(["COMM-PPO-01", "COMM-HMO-02", "COMM-HDHP-03", "MEDD-PDP-04", "MEDD-MAPD-05", "MCAID-06"])
PLAN_P = np.array([0.30, 0.22, 0.13, 0.15, 0.10, 0.10])

member_ids = np.array([f"MBR{1_000_000 + i}" for i in range(N_MEMBERS)], dtype=object)
plan = rng.choice(PLANS, size=N_MEMBERS, p=PLAN_P)
is_medicare = np.char.startswith(plan.astype(str), "MEDD")
birth_year = np.where(is_medicare, rng.integers(1935, 1960, N_MEMBERS), rng.integers(1955, 2023, N_MEMBERS))
gender = rng.choice(["F", "M", "U"], size=N_MEMBERS, p=[0.51, 0.48, 0.01])

# Start month: 55% before the claim period (2018-2023), 45% during it (2024-01 .. 2025-10)
m_pre = rng.integers(np.datetime64("2018-01", "M").astype(int), np.datetime64("2024-01", "M").astype(int), N_MEMBERS)
m_in = rng.integers(np.datetime64("2024-01", "M").astype(int), np.datetime64("2025-11", "M").astype(int), N_MEMBERS)
start_m = np.where(rng.random(N_MEMBERS) < 0.55, m_pre, m_in)
# 25% have termed: 6-36 months after starting, but not before Feb-2024 (everyone overlaps the period)
termed = rng.random(N_MEMBERS) < 0.25
end_m = np.maximum(start_m + rng.integers(6, 37, N_MEMBERS), np.datetime64("2024-02", "M").astype(int))
elig_start = start_m.astype("datetime64[M]").astype("datetime64[D]").astype(int)
elig_end = (end_m + 1).astype("datetime64[M]").astype("datetime64[D]").astype(int) - 1  # last day of end month

members = pd.DataFrame({
    "member_id": member_ids,
    "plan_id": plan,
    "birth_year": birth_year,
    "gender": gender,
    "state": rng.choice(STATES, size=N_MEMBERS, p=STATE_P),
    "eligibility_start": days_to_str(elig_start),
    "eligibility_end": np.where(termed, days_to_str(elig_end), None),
})

# Window inside the claim period when each member may legitimately fill
win_lo = np.maximum(elig_start, P0)
win_hi = np.where(termed, np.minimum(elig_end, P1), P1)
assert (win_hi >= win_lo).all()

# COMMAND ----------

# MAGIC %md ## 4. Original paid claims
# MAGIC Each claim picks a drug (weighted by popularity), then a member (heavy-tailed utilisation, scaled by how long they are eligible),
# MAGIC then a fill date inside that member's eligibility. The dispensing channel follows the drug: specialty drugs mostly go through
# MAGIC specialty pharmacies, and some maintenance drugs go to mail order with 90-day fills.

# COMMAND ----------

n = N_BASE_CLAIMS

# Drug
d_idx = rng.choice(len(drugs), size=n, p=drugs["_claim_weight"].to_numpy())
d_spec = drugs.specialty_flag.to_numpy()[d_idx] == "Y"
d_brand = drugs.brand_generic_flag.to_numpy()[d_idx] == "B"
d_class = drugs.therapeutic_class.to_numpy()[d_idx]

# Member: gamma-distributed utilisation x days eligible in the period
m_weight = rng.gamma(0.7, size=N_MEMBERS) * (win_hi - win_lo + 1)
m_idx = rng.choice(N_MEMBERS, size=n, p=m_weight / m_weight.sum())
fill = win_lo[m_idx] + (rng.random(n) * (win_hi[m_idx] - win_lo[m_idx] + 1)).astype(np.int64)

# Channel -> pharmacy
idx_by_type = {t: np.flatnonzero(ph_type == t) for t in ("retail", "specialty", "mail")}
u = rng.random(n)
channel = np.where(d_spec, np.where(u < 0.85, "specialty", "retail"), np.where(u < 0.08, "mail", "retail"))
ph_idx = np.empty(n, dtype=np.int64)
for t, pool in idx_by_type.items():
    mask = channel == t
    ph_idx[mask] = rng.choice(pool, size=mask.sum())

# Days supply and quantity
is_anti_inf = d_class == "Anti-infectives"
days_supply = np.select(
    [channel == "mail", d_spec, is_anti_inf],
    [90, 30, rng.choice([5, 7, 10, 14], size=n)],
    default=np.where(rng.random(n) < 0.8, 30, 90),
)
qty = np.maximum(1, np.round(drugs["_qty_per_30"].to_numpy()[d_idx] * days_supply / 30)).astype(np.int64)

# Money: ingredient cost from unit cost (mail gets a 10% discount), fee by channel, copay by benefit tier
ingredient_cost = qty * drugs["_unit_cost"].to_numpy()[d_idx] * rng.uniform(0.97, 1.03, n) * np.where(channel == "mail", 0.9, 1.0)
dispensing_fee = np.select([channel == "mail", channel == "specialty"],
                           [rng.uniform(0, 1, n), rng.uniform(0, 5, n)], default=rng.uniform(0.5, 2.5, n))
is_90 = days_supply >= 90
copay = np.select(
    [d_spec, d_brand],
    [np.minimum(0.2 * ingredient_cost, 250), np.where(is_90, 100, 40)],  # specialty 20% coinsurance capped at $250
    default=np.where(is_90, 20, 10),  # generic tier
).astype(float)
ingredient_cost, dispensing_fee = ingredient_cost.round(2), dispensing_fee.round(2)
copay = np.minimum(copay, ingredient_cost + dispensing_fee).round(2)  # member never pays more than the claim
plan_paid = (ingredient_cost + dispensing_fee - copay).round(2)

base = pd.DataFrame({
    "_fill_day": fill,
    "rx_number": rng.integers(1_000_000, 10_000_000, n).astype(str),
    "member_id": member_ids[m_idx],
    "pharmacy_npi": pharmacy_npis[ph_idx],
    "prescriber_npi": rng.choice(prescriber_npis, size=n),
    "_d_idx": d_idx,
    "quantity_dispensed": qty,
    "days_supply": days_supply,
    "ingredient_cost": ingredient_cost,
    "dispensing_fee": dispensing_fee,
    "copay": copay,
    "plan_paid": plan_paid,
})

# The business key must be unique among *originals*; otherwise dedupe would remove legitimate claims and the answer key would be off.
n_key_collisions = int(base.duplicated(["rx_number", "_fill_day", "pharmacy_npi"]).sum())
base = base.drop_duplicates(["rx_number", "_fill_day", "pharmacy_npi"]).reset_index(drop=True)
n = len(base)
print(f"{n:,} original claims ({n_key_collisions} random key collisions dropped)")

# COMMAND ----------

# MAGIC %md ## 5. Inject row-level messiness
# MAGIC Disjoint slices of one random permutation, so each injected row has exactly one problem and the manifest counts are exact.

# COMMAND ----------

perm = rng.permutation(n)
cuts = np.cumsum([int(n * r) for r in (RATE_OUTSIDE_ELIGIBILITY, RATE_UNKNOWN_MEMBER, RATE_UNKNOWN_PHARMACY, RATE_NULL_PRESCRIBER)])
rows_outside, rows_unk_mem, rows_unk_ph, rows_null_presc = np.split(perm[: cuts[-1]], cuts[:-1])

# Outside eligibility: move the claim to a member with a coverage gap inside the period and pick a date in that gap
gap_before = np.flatnonzero(elig_start > P0 + 30)  # enrolled mid-period, so the period has days before their start
gap_after = np.flatnonzero(termed & (elig_end < P1 - 30))  # termed mid-period, so the period has days after their end
gap_member = np.concatenate([gap_before, gap_after])
gap_lo = np.concatenate([np.full(len(gap_before), P0), elig_end[gap_after] + 1])
gap_hi = np.concatenate([elig_start[gap_before] - 1, np.full(len(gap_after), P1)])
g = rng.integers(0, len(gap_member), len(rows_outside))
base.loc[rows_outside, "member_id"] = member_ids[gap_member[g]]
base.loc[rows_outside, "_fill_day"] = gap_lo[g] + (rng.random(len(g)) * (gap_hi[g] - gap_lo[g] + 1)).astype(np.int64)

# Unknown member / pharmacy (ids that look valid but aren't in the reference files), null prescriber
base.loc[rows_unk_mem, "member_id"] = [f"MBR{x}" for x in rng.integers(8_000_000, 9_000_000, len(rows_unk_mem))]
base.loc[rows_unk_ph, "pharmacy_npi"] = [f"1{x:09d}" for x in rng.integers(0, 10**9, len(rows_unk_ph))]
base.loc[rows_null_presc, "prescriber_npi"] = None

# Re-check the key after moving fill dates (very unlikely to collide, but keep the answer key exact)
base = base.drop_duplicates(["rx_number", "_fill_day", "pharmacy_npi"]).reset_index(drop=True)
n = len(base)

# Claim ids increase with fill date, as an adjudication system would issue them
base = base.sort_values(["_fill_day", "rx_number"]).reset_index(drop=True)
base.insert(0, "claim_id", [f"CLM{i:010d}" for i in range(1, n + 1)])
next_claim_seq = n + 1

# COMMAND ----------

# MAGIC %md ## 6. Mixed NDC formats
# MAGIC Formats are applied to originals *before* reversals and duplicates are copied, so a reversal carries the same NDC string as
# MAGIC the claim it reverses, just as a real resubmission would.

# COMMAND ----------

fmt_keys = list(NDC_FORMAT_MIX)
fmt = rng.choice(fmt_keys, size=n, p=list(NDC_FORMAT_MIX.values()))
d = base["_d_idx"].to_numpy()
ndc11 = drugs.ndc11.to_numpy()[d]
# Leading zeros can only be lost if there are leading zeros (4-digit labelers). Ineligible picks become plain, then
# eligible plain rows are topped up so the overall rate still hits the target.
has_zero = np.char.startswith(ndc11.astype(str), "0")
fmt = np.where((fmt == "leading_zeros_lost") & ~has_zero, "ndc11_plain", fmt)
shortfall = int(n * NDC_FORMAT_MIX["leading_zeros_lost"]) - int((fmt == "leading_zeros_lost").sum())
top_up_pool = np.flatnonzero((fmt == "ndc11_plain") & has_zero)
fmt[rng.choice(top_up_pool, size=min(max(shortfall, 0), len(top_up_pool)), replace=False)] = "leading_zeros_lost"

INVALID_NDC_VALUES = np.array([None, "", "UNKNOWN", "N/A", "12345", "1234-56", "ABC12-3456-78", "123456789012345"], dtype=object)
# Well-formed NDCs using labelers that no manufacturer in the reference owns, so they cannot map
known_labelers = {c.zfill(5) for c in labeler_code.values()}
unmapped_pool = []
while len(unmapped_pool) < 60:
    cand = f"{rng.integers(0, 10**11):011d}"
    if cand[:5] not in known_labelers:
        unmapped_pool.append(cand)
unmapped_pool = np.array(unmapped_pool, dtype=object)

base["ndc"] = np.select(
    [fmt == "ndc11_plain", fmt == "ndc10_dashed", fmt == "ndc11_dashed", fmt == "leading_zeros_lost", fmt == "invalid_format"],
    [ndc11, drugs["_ndc10_dashed"].to_numpy()[d], drugs["_ndc11_dashed"].to_numpy()[d],
     drugs["_ndc11_no_zeros"].to_numpy()[d], rng.choice(INVALID_NDC_VALUES, size=n)],
    default=rng.choice(unmapped_pool, size=n),  # not_in_reference
)
ndc_format_counts = pd.Series(fmt).value_counts().to_dict()

# Delivery month: normally the fill month; a slice arrives only in the late file
base["_delivery"] = delivery_label(base["_fill_day"].to_numpy())
late_rows = rng.choice(n, int(n * RATE_LATE), replace=False)
base.loc[late_rows, "_delivery"] = LATE_DELIVERY
base["claim_status"] = "paid"
base["reversal_of_claim_id"] = None

# COMMAND ----------

# MAGIC %md ## 7. Reversals, rebills, duplicates
# MAGIC * **Reversal** – new `claim_id`, `claim_status='reversed'`, same `rx_number`/`fill_date`/`pharmacy_npi`, **negated amounts**,
# MAGIC   and `reversal_of_claim_id` -> original. It arrives 1-30 days after the fill, sometimes in the next month's file.
# MAGIC * **Rebill** – a corrected paid claim with a new `claim_id` on the **same business key**, submitted after the reversal.
# MAGIC * **Exact duplicate** – the same row written twice in one file.
# MAGIC * **Resend** – the same claim again in the late file. Some get a new `claim_id`, and some have a corrected copay.
# MAGIC
# MAGIC Because a reversal and a rebill share the business key with the original, **silver must net reversals before deduplicating**.
# MAGIC Otherwise a key-only dedupe would collapse paid + reversed + rebill into one arbitrary row.

# COMMAND ----------


def new_ids(k: int) -> list:
    global next_claim_seq
    ids = [f"CLM{i:010d}" for i in range(next_claim_seq, next_claim_seq + k)]
    next_claim_seq += k
    return ids


def negate_money(df: pd.DataFrame) -> pd.DataFrame:
    df[MONEY_COLS] = -df[MONEY_COLS] + 0.0  # + 0.0 turns -0.0 into 0.0 so the CSV never shows "-0.0"
    return df


# Reversals
rev_src = rng.choice(n, int(n * RATE_REVERSAL), replace=False)
rev = base.iloc[rev_src].copy()
rev_day = rev["_fill_day"].to_numpy() + rng.integers(1, 31, len(rev))
rev["reversal_of_claim_id"] = rev["claim_id"].to_numpy()
rev["claim_id"] = new_ids(len(rev))
rev["claim_status"] = "reversed"
rev["_delivery"] = delivery_label(rev_day)
rev = negate_money(rev)
n_rev_before_orig = int((rev["_delivery"].to_numpy() < base["_delivery"].to_numpy()[rev_src]).sum())

# Rebills: corrected quantity/cost, submitted 0-5 days after the reversal
rb_pick = rng.random(len(rev_src)) < RATE_REBILL
rebill = base.iloc[rev_src[rb_pick]].copy()
rebill["claim_id"] = new_ids(len(rebill))
rebill["ingredient_cost"] = (rebill["ingredient_cost"] * rng.uniform(0.85, 1.0, len(rebill))).round(2)
rebill["copay"] = np.minimum(rebill["copay"], rebill["ingredient_cost"] + rebill["dispensing_fee"]).round(2)
rebill["plan_paid"] = (rebill["ingredient_cost"] + rebill["dispensing_fee"] - rebill["copay"]).round(2)
rebill["_delivery"] = delivery_label(rev_day[rb_pick] + rng.integers(0, 6, len(rebill)))

# Exact duplicates: same row, same file
exact_dup = base.iloc[rng.choice(n, int(n * RATE_EXACT_DUP), replace=False)].copy()

# Resends in the late file. Only drawn from claims that were NOT reversed: a resend with a new claim_id of a reversed claim
# would survive netting (its claim_id isn't the one reversed) and bring a cancelled claim back as paid.
not_reversed = np.setdiff1d(np.arange(n), rev_src)
resend = base.iloc[rng.choice(not_reversed, int(n * RATE_RESEND_DUP), replace=False)].copy()
k = len(resend)
new_id_mask = rng.random(k) < RATE_RESEND_NEW_ID
corrected_mask = rng.random(k) < RATE_RESEND_CORRECTED
resend.loc[new_id_mask, "claim_id"] = new_ids(int(new_id_mask.sum()))
resend.loc[corrected_mask, "copay"] = (resend.loc[corrected_mask, "copay"] * rng.uniform(0.5, 0.9, corrected_mask.sum())).round(2)
resend["plan_paid"] = (resend["ingredient_cost"] + resend["dispensing_fee"] - resend["copay"]).round(2)
resend["_delivery"] = LATE_DELIVERY

# Orphan reversals: point at claim ids that were never issued
orphan = base.iloc[rng.choice(n, N_ORPHAN_REVERSALS, replace=False)].copy()
orphan["claim_id"] = new_ids(len(orphan))
orphan["reversal_of_claim_id"] = [f"CLM9{x:09d}" for x in rng.integers(0, 10**9, len(orphan))]
orphan["claim_status"] = "reversed"
orphan["_delivery"] = delivery_label(orphan["_fill_day"].to_numpy() + rng.integers(1, 31, len(orphan)))
orphan = negate_money(orphan)

claims = pd.concat([base, rev, rebill, exact_dup, resend, orphan], ignore_index=True)
claims["fill_date"] = days_to_str(claims["_fill_day"].to_numpy())
claims = claims.sort_values(["_delivery", "_fill_day", "claim_id"], kind="stable").reset_index(drop=True)
print(f"{len(claims):,} claim rows in total")

# COMMAND ----------

# MAGIC %md ## 8. Rebate contracts
# MAGIC Brand manufacturers only, since generics are rarely rebated. Two contract grains, matching the CLAUDE.md spec:
# MAGIC * **NDC-level** (`ndc11` set, `therapeutic_class` blank): higher %, annual terms. Some lapse at the end of 2024 and are not renewed.
# MAGIC * **Class-level** (`therapeutic_class` set, `ndc11` blank): a lower % for every brand NDC the manufacturer sells in that class.
# MAGIC
# MAGIC A claim can match **both** grains. Gold needs a precedence rule (NDC-level beats class-level), or rebates get counted twice.
# MAGIC `rebate_pct` is a percentage (22.5 means 22.5%).

# COMMAND ----------

brand_drugs = drugs[drugs.brand_generic_flag == "B"]
contracts = []
for mfr, grp in brand_drugs.groupby("manufacturer"):
    # NDC-level: the manufacturer's 3 most-used brand NDCs
    for _, row in grp.nlargest(3, "_claim_weight").iterrows():
        pct = rng.uniform(5, 15) if row.specialty_flag == "Y" else rng.uniform(15, 40)
        contracts.append((mfr, row.ndc11, None, round(pct, 1), "2024-01-01", "2024-12-31"))
        if rng.random() < 0.8:  # 20% lapse and are not renewed for 2025
            contracts.append((mfr, row.ndc11, None, round(pct * rng.uniform(0.9, 1.15), 1), "2025-01-01", "2025-12-31"))
    # Class-level: one class the manufacturer sells brands in, starting on a quarter boundary
    cls = str(rng.choice(grp.therapeutic_class.unique()))
    start = str(rng.choice(["2024-01-01", "2024-04-01", "2024-07-01"]))
    contracts.append((mfr, None, cls, round(rng.uniform(3, 12), 1), start, "2025-12-31"))

rebate_contracts = pd.DataFrame(
    contracts, columns=["manufacturer", "ndc11", "therapeutic_class", "rebate_pct", "effective_start", "effective_end"])
rebate_contracts.insert(0, "contract_id", [f"RC-{i:04d}" for i in range(1, len(rebate_contracts) + 1)])
print(f"{len(rebate_contracts)} rebate contracts")

# COMMAND ----------

# MAGIC %md ## 9. Write files to the landing volume

# COMMAND ----------

SUBFOLDERS = ["claims", "drug_reference", "members", "pharmacies", "rebate_contracts", "_manifest"]
late_file = f"claims/claims_delivery_{LATE_DELIVERY}.csv"

if WRITE_MODE in ("all", "initial_only"):
    # Clear previous output so a re-run never leaves stale files behind (idempotent).
    for sub in SUBFOLDERS:
        dbutils.fs.rm(f"{LANDING}/{sub}", True)
    write_csv(drugs[[c for c in drugs.columns if not c.startswith("_")]], "drug_reference/drug_reference.csv")
    write_csv(members, "members/members.csv")
    write_csv(pharmacies, "pharmacies/pharmacies.csv")
    write_csv(rebate_contracts, "rebate_contracts/rebate_contracts.csv")

deliveries = sorted(claims["_delivery"].unique())
for dlv in deliveries:
    is_late = dlv == LATE_DELIVERY
    if (WRITE_MODE == "initial_only" and is_late) or (WRITE_MODE == "late_only" and not is_late):
        continue
    write_csv(claims.loc[claims["_delivery"] == dlv, CLAIM_COLS], f"claims/claims_delivery_{dlv}.csv")

# COMMAND ----------

# MAGIC %md ## 10. Manifest (answer key)
# MAGIC Silver and gold reconcile against these numbers, e.g. "we injected N exact duplicates and silver removed N".

# COMMAND ----------

manifest = {
    "seed": SEED,
    "period": [str(PERIOD_START), str(PERIOD_END)],
    "row_counts": {
        "claims_total": len(claims),
        "original_paid_claims": n,
        "reversals": len(rev),
        "rebills": len(rebill),
        "exact_duplicates": len(exact_dup),
        "resends": len(resend),
        "resends_with_new_claim_id": int(new_id_mask.sum()),
        "resends_with_corrected_copay": int(corrected_mask.sum()),
        "orphan_reversals": len(orphan),
        "drug_reference": len(drugs),
        "members": len(members),
        "pharmacies": len(pharmacies),
        "rebate_contracts": len(rebate_contracts),
    },
    "injected_on_original_claims": {
        "outside_eligibility": len(rows_outside),
        "unknown_member": len(rows_unk_mem),
        "unknown_pharmacy": len(rows_unk_ph),
        "null_prescriber": len(rows_null_presc),
        "late_arrivals": len(late_rows),
        "reversals_delivered_before_original": n_rev_before_orig,
        "ndc_formats": {k: int(v) for k, v in ndc_format_counts.items()},
    },
    "expected_after_silver": {
        # originals - reversed originals + rebills, before rejecting invalid NDCs (those are counted in ndc_formats)
        "final_paid_claims_before_ndc_rejects": n - len(rev) + len(rebill),
    },
    "claim_files": {f"claims_delivery_{d}.csv": int((claims["_delivery"] == d).sum()) for d in deliveries},
}
if WRITE_MODE in ("all", "initial_only"):
    os.makedirs(f"{LANDING}/_manifest", exist_ok=True)
    with open(f"{LANDING}/_manifest/generation_manifest.json", "w") as fh:
        json.dump(manifest, fh, indent=2)
print(json.dumps({k: v for k, v in manifest.items() if k != "claim_files"}, indent=2))

# COMMAND ----------

# MAGIC %md ## 11. Quick check: read back with Spark
# MAGIC Read as **all strings** (no schema inference) so the NDC formats show exactly as written. Bronze will do the same.

# COMMAND ----------

from pyspark.sql import functions as F

check = (
    spark.read.option("header", True).option("inferSchema", False).csv(f"{LANDING}/claims")
    .select("*", F.col("_metadata.file_name").alias("file_name"))
)
display(check.groupBy("file_name").agg(F.count("*").alias("rows")).orderBy("file_name"))

# COMMAND ----------

display(check.select("claim_id", "ndc", "claim_status", "reversal_of_claim_id", "copay", "plan_paid").limit(20))
