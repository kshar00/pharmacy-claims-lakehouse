"""Checks that the repo's pieces still agree with each other: job definition, notebooks,
dashboard export, dataset SQL and documentation references. These catch drift that would
otherwise only show up as a failed job or a broken link."""
import ast
import json
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent
NOTEBOOKS = REPO / "notebooks"
EXPECTED_TASKS = ["setup_catalog", "generate_synthetic_data", "bronze_ingest",
                  "silver_claims", "silver_dq", "gold_marts"]


def _job():
    return json.loads((REPO / "jobs" / "pipeline_job.json").read_text(encoding="utf-8"))


# ---- Job definition ---------------------------------------------------------------

def test_job_tasks_run_in_a_straight_line():
    tasks = _job()["tasks"]
    assert [t["task_key"] for t in tasks] == EXPECTED_TASKS
    for previous, task in zip(tasks, tasks[1:]):
        assert task["depends_on"] == [{"task_key": previous["task_key"]}], task["task_key"]


def test_every_job_task_points_at_an_existing_notebook():
    for task in _job()["tasks"]:
        name = task["notebook_task"]["notebook_path"].rsplit("/", 1)[-1]
        matches = list(NOTEBOOKS.glob(name + ".*"))
        assert matches, f"{task['task_key']}: no notebook named {name} in notebooks/"


def test_job_is_serverless_and_safe_to_rerun():
    job = _job()
    assert job["max_concurrent_runs"] == 1
    assert job["schedule"]["pause_status"] == "PAUSED"
    for task in job["tasks"]:
        assert "new_cluster" not in task and "existing_cluster_id" not in task and "job_cluster_key" not in task
    gold = next(t for t in job["tasks"] if t["task_key"] == "gold_marts")
    assert gold.get("run_if", "ALL_SUCCESS") == "ALL_SUCCESS", "gold must be gated by data quality"


def test_job_parameters_include_run_id():
    params = {p["name"]: p["default"] for p in _job()["parameters"]}
    assert params.get("run_id") == "{{job.run_id}}"


# ---- Notebooks --------------------------------------------------------------------

def test_notebooks_have_databricks_source_headers():
    for path in sorted(NOTEBOOKS.iterdir()):
        first = path.read_text(encoding="utf-8").splitlines()[0]
        expected = "# Databricks notebook source" if path.suffix == ".py" else "-- Databricks notebook source"
        assert first == expected, path.name


def test_python_notebooks_are_valid_python():
    for path in sorted(NOTEBOOKS.glob("*.py")):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


# ---- Dashboard --------------------------------------------------------------------

def _normalise_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r";\s*$", "", sql.strip()))


def test_dashboard_export_matches_dataset_sql_files():
    dashboard = json.loads((REPO / "dashboards" / "pharmacy_claims_lakehouse.lvdash.json").read_text(encoding="utf-8"))
    exported = {d["displayName"]: d["config"]["source"] for d in dashboard["datasets"]}
    files = {re.sub(r"^\d+_", "", p.stem): p for p in (REPO / "dashboards" / "datasets").glob("*.sql")}
    assert set(exported) == set(files), "dataset names differ between the export and datasets/*.sql"
    for name, path in files.items():
        assert _normalise_sql(exported[name]) == _normalise_sql(path.read_text(encoding="utf-8")), (
            f"{name}: dashboard SQL differs from {path.name}. Re-export the dashboard or update the file.")


def test_dashboard_export_has_no_personal_details():
    text = (REPO / "dashboards" / "pharmacy_claims_lakehouse.lvdash.json").read_text(encoding="utf-8")
    assert not re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", text), "email address in the dashboard export"
    assert "cloud.databricks.com" not in text, "workspace URL in the dashboard export"


# ---- Documentation ----------------------------------------------------------------

def test_design_decision_references_exist():
    decisions = set(re.findall(r"^### (D\d+)\.", (REPO / "docs" / "design_decisions.md").read_text(encoding="utf-8"), re.M))
    for doc in [REPO / "README.md", REPO / "docs" / "architecture.md", REPO / "dashboards" / "README.md"]:
        for ref in re.findall(r"\bD\d+\b", doc.read_text(encoding="utf-8")):
            assert ref in decisions, f"{doc.name} cites {ref}, which isn't in design_decisions.md"


def test_relative_markdown_links_resolve():
    for doc in [REPO / "README.md", *(REPO / "docs").glob("*.md"), REPO / "dashboards" / "README.md"]:
        for link in re.findall(r"\]\(([^)#\s]+)\)", doc.read_text(encoding="utf-8")):
            if link.startswith(("http://", "https://", "mailto:")):
                continue
            assert (doc.parent / link).exists(), f"{doc.relative_to(REPO)}: broken link {link}"
