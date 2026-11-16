"""Shared test fixtures.

The notebooks are Databricks source files, not importable modules. The `generator` fixture
runs the generator notebook's setup and helper cells (everything before "## 1. Drug reference")
with a stand-in for `dbutils`, so the helper functions can be tested locally and in CI
without Databricks or Spark. No data is generated or written.
"""
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
GENERATOR = REPO / "notebooks" / "01_generate_synthetic_data.py"
STOP_MARKER = "# MAGIC %md ## 1. Drug reference"


class _Widgets:
    DEFAULTS = {"catalog": "portfolio", "write_mode": "all", "random_seed": "42"}

    def text(self, *args): pass
    def dropdown(self, *args): pass
    def get(self, name): return self.DEFAULTS[name]


class _DbUtils:
    widgets = _Widgets()


def load_generator_helpers() -> dict:
    """Execute the generator's parameter, configuration and helper cells; return its namespace."""
    source = GENERATOR.read_text(encoding="utf-8")
    assert STOP_MARKER in source, "Generator layout changed: update STOP_MARKER in tests/conftest.py"
    namespace = {"dbutils": _DbUtils(), "__name__": "generator_under_test"}
    exec(compile(source.split(STOP_MARKER)[0], str(GENERATOR), "exec"), namespace)
    return namespace


@pytest.fixture
def generator() -> dict:
    return load_generator_helpers()
