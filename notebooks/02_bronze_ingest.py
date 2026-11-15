# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze ingest
# MAGIC
# MAGIC Loads every landing folder into its own Delta table in `portfolio.bronze`, **as-is**:
# MAGIC
# MAGIC | Landing folder | Bronze table | File pattern |
# MAGIC |---|---|---|
# MAGIC | `landing/claims/` | `bronze.claims` | monthly delivery files + late file |
# MAGIC | `landing/drug_reference/` | `bronze.drug_reference` | full snapshot |
# MAGIC | `landing/members/` | `bronze.members` | full snapshot |
# MAGIC | `landing/pharmacies/` | `bronze.pharmacies` | full snapshot |
# MAGIC | `landing/rebate_contracts/` | `bronze.rebate_contracts` | full snapshot |
# MAGIC
# MAGIC ### What bronze does and does not do
# MAGIC * **Does:** load each file once, keep every row and every column, and add lineage columns:
# MAGIC   | Column | Meaning |
# MAGIC   |---|---|
# MAGIC   | `_ingested_at` | When this load ran (one value per micro-batch) |
# MAGIC   | `_source_file` | Full volume path of the file the row came from |
# MAGIC   | `_source_file_modified_at` | When the file landed. Tie-breaker for "latest wins" when several files load in one run |
# MAGIC   | `_run_id` | Job run id (or `manual`), so `gold.pipeline_audit` can count rows per run |
# MAGIC   | `_rescued_data` | Anything that didn't fit the expected columns (should be empty) |
# MAGIC * **Does not:** cast types, fix NDCs, dedupe or filter. All of that is silver's job, so bronze can always be replayed.
# MAGIC
# MAGIC ### How it works
# MAGIC **Auto Loader** (`cloudFiles`) with `trigger(availableNow=True)`: each run processes only files it has not seen before, then stops.
# MAGIC That is incremental batch, which is what serverless supports and what fits the daily quota (no always-on stream).
# MAGIC Which files have been loaded is tracked in a checkpoint in the `raw.checkpoints` volume.
# MAGIC
# MAGIC **Re-running is safe:** a second run with no new files loads 0 rows. Bronze tables are append-only (`delta.appendOnly = true`).
# MAGIC To start over, set `reset = true`: it drops the bronze tables and their checkpoints, then reloads everything.

# COMMAND ----------

# MAGIC %md ## Parameters
# MAGIC * `catalog` – same as notebook 01.
# MAGIC * `run_id` – leave as `manual` when running by hand. The Job (Step 6) passes `{{job.run_id}}`.
# MAGIC * `reset` – `true` drops bronze tables and checkpoints first (full reload). Leave `false` normally.

# COMMAND ----------

dbutils.widgets.text("catalog", "portfolio", "Catalog")
dbutils.widgets.text("run_id", "manual", "Run id")
dbutils.widgets.dropdown("reset", "false", ["false", "true"], "Reset bronze (full reload)")

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = dbutils.widgets.get("run_id")
RESET = dbutils.widgets.get("reset") == "true"

LANDING = f"/Volumes/{CATALOG}/raw/landing"
STATE = f"/Volumes/{CATALOG}/raw/checkpoints/bronze"
print(f"catalog={CATALOG} | run_id={RUN_ID} | reset={RESET}")

# COMMAND ----------

# MAGIC %md ## Source definitions
# MAGIC One entry per source. `expected_columns` is the contract with the source system: if a file arrives without one of these
# MAGIC columns the notebook fails loudly instead of loading a half-empty table. Extra columns don't fail the load; they land in `_rescued_data`.

# COMMAND ----------

SOURCES = {
    "claims": {
        "expected_columns": [
            "claim_id", "rx_number", "fill_date", "member_id", "pharmacy_npi", "prescriber_npi", "ndc",
            "quantity_dispensed", "days_supply", "ingredient_cost", "dispensing_fee", "copay", "plan_paid",
            "claim_status", "reversal_of_claim_id",
        ],
        "comment": "Bronze: raw pharmacy claims (paid + reversed) from monthly delivery files and late files. All columns are strings, as received.",
    },
    "drug_reference": {
        "expected_columns": [
            "ndc11", "drug_name", "brand_generic_flag", "specialty_flag", "therapeutic_class",
            "manufacturer", "package_size", "unit_of_measure",
        ],
        "comment": "Bronze: drug reference snapshots (one row per ndc11 per file). All columns are strings, as received.",
    },
    "members": {
        "expected_columns": ["member_id", "plan_id", "birth_year", "gender", "state", "eligibility_start", "eligibility_end"],
        "comment": "Bronze: member eligibility snapshots. Null eligibility_end = still active. All columns are strings, as received.",
    },
    "pharmacies": {
        "expected_columns": ["pharmacy_npi", "name", "type", "state"],
        "comment": "Bronze: pharmacy directory snapshots (retail / specialty / mail). All columns are strings, as received.",
    },
    "rebate_contracts": {
        "expected_columns": [
            "contract_id", "manufacturer", "ndc11", "therapeutic_class", "rebate_pct", "effective_start", "effective_end",
        ],
        "comment": "Bronze: manufacturer rebate contracts, NDC-level or class-level. All columns are strings, as received.",
    },
}

# COMMAND ----------

# MAGIC %md ## Setup and optional reset

# COMMAND ----------

# Also created in notebook 00. Repeated here (IF NOT EXISTS) so this notebook still works if 00 was run before Step 2 existed.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.checkpoints "
          "COMMENT 'Auto Loader checkpoints and schema tracking for bronze loads. Pipeline state, not data.'")

if RESET:
    for name in SOURCES:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.{name}")
        dbutils.fs.rm(f"{STATE}/{name}", True)  # checkpoint + schema; without this, Auto Loader would skip every file again
    print("Reset done: bronze tables and checkpoints removed.")

# COMMAND ----------

# MAGIC %md ## Load function
# MAGIC Key Auto Loader options, and why:
# MAGIC * `cloudFiles.inferColumnTypes = false` – every column stays a **string**. Type inference would turn `ndc` into a number and drop
# MAGIC   leading zeros, and would fail on junk values like `UNKNOWN`. Casting is a silver decision.
# MAGIC * `cloudFiles.schemaEvolutionMode = rescue` – the schema is fixed from the first files. Unexpected columns go to `_rescued_data`
# MAGIC   instead of stopping the load, so a source-system change is captured, not lost and not fatal.
# MAGIC * `pathGlobFilter = *.csv` – ignore stray files such as `.crc`, notes, or a stray `.xlsx` dropped in the folder.
# MAGIC * `trigger(availableNow=True)` – process everything new, then stop. Serverless doesn't run continuous streams.

# COMMAND ----------

from pyspark.sql import functions as F


def load_to_bronze(name: str, cfg: dict) -> dict:
    table = f"{CATALOG}.bronze.{name}"
    state = f"{STATE}/{name}"
    rows_before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", f"{state}/_schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("pathGlobFilter", "*.csv")
        .load(f"{LANDING}/{name}")
        .select(
            "*",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.lit(RUN_ID).alias("_run_id"),
        )
    )
    query = (
        stream.writeStream
        .option("checkpointLocation", f"{state}/_checkpoint")
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()

    # Contract check: fail loudly if an expected column never arrived
    missing = sorted(set(cfg["expected_columns"]) - set(spark.table(table).columns))
    if missing:
        raise ValueError(f"{table} is missing expected columns: {missing}")

    # Table metadata (idempotent). appendOnly makes bronze immutable: UPDATE/DELETE are rejected.
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    spark.sql(f"COMMENT ON TABLE {table} IS '{cfg['comment']}'")

    rows_after = spark.table(table).count()
    return {"table": table, "rows_before": rows_before, "rows_loaded_this_run": rows_after - rows_before, "rows_total": rows_after}

# COMMAND ----------

# MAGIC %md ## Run the loads
# MAGIC Run one source after another, not in parallel. Five small loads take a minute or two, and running them in sequence keeps quota use flat.

# COMMAND ----------

results = [load_to_bronze(name, cfg) for name, cfg in SOURCES.items()]
display(spark.createDataFrame(results))

# COMMAND ----------

# MAGIC %md ## Reconcile against the generator's manifest
# MAGIC Every claim file's bronze row count must equal what the generator says it wrote. This is the first link in the audit chain
# MAGIC (`landing -> bronze -> silver -> gold`) that `gold.pipeline_audit` will report on.
# MAGIC
# MAGIC | Status | Meaning |
# MAGIC |---|---|
# MAGIC | `MATCH` | Loaded once, row count matches |
# MAGIC | `NOT_LOADED` | In the manifest but not in bronze yet (expected for the late file after `write_mode = initial_only`) |
# MAGIC | `MISMATCH` | Row count differs. Investigate (for example, a file loaded twice after a partial reset) |
# MAGIC | `UNEXPECTED_FILE` | In bronze but not in the manifest |

# COMMAND ----------

import json

with open(f"{LANDING}/_manifest/generation_manifest.json") as fh:
    manifest = json.load(fh)

expected = spark.createDataFrame(list(manifest["claim_files"].items()), "file_name string, expected_rows long")
actual = (
    spark.table(f"{CATALOG}.bronze.claims")
    .groupBy(F.element_at(F.split("_source_file", "/"), -1).alias("file_name"))
    .agg(F.count("*").alias("bronze_rows"))
)
recon = (
    expected.join(actual, "file_name", "full_outer")
    .withColumn(
        "status",
        F.when(F.col("expected_rows").isNull(), "UNEXPECTED_FILE")
        .when(F.col("bronze_rows").isNull(), "NOT_LOADED")
        .when(F.col("expected_rows") == F.col("bronze_rows"), "MATCH")
        .otherwise("MISMATCH"),
    )
    .orderBy("file_name")
)
display(recon)

bad = recon.filter(F.col("status").isin("MISMATCH", "UNEXPECTED_FILE")).count()
print("Claims reconciliation:", "PASS" if bad == 0 else f"FAIL ({bad} files)")

# COMMAND ----------

# Reference tables: total rows vs manifest (one snapshot each, unless new snapshot files have been dropped in)
ref = [
    (name, manifest["row_counts"][name], spark.table(f"{CATALOG}.bronze.{name}").count())
    for name in SOURCES if name != "claims"
]
display(spark.createDataFrame(ref, "table string, expected_rows long, bronze_rows long")
        .withColumn("status", F.when(F.col("expected_rows") == F.col("bronze_rows"), "MATCH").otherwise("CHECK")))

# COMMAND ----------

# MAGIC %md ## Spot checks
# MAGIC 1. `_rescued_data` should be empty everywhere (all files had exactly the expected columns).
# MAGIC 2. NDCs are still exactly as received: leading zeros kept, dashes kept, junk values kept.

# COMMAND ----------

display(spark.createDataFrame(
    [(name, spark.table(f"{CATALOG}.bronze.{name}").filter("_rescued_data IS NOT NULL").count()) for name in SOURCES],
    "table string, rows_with_rescued_data long",
))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      CASE
        WHEN ndc IS NULL                         THEN 'null'
        WHEN ndc RLIKE '^[0-9]{{11}}$'           THEN '11-digit plain'
        WHEN ndc RLIKE '^[0-9]{{5}}-[0-9]{{4}}-[0-9]{{2}}$' THEN '11-digit dashed (5-4-2)'
        WHEN ndc RLIKE '^[0-9]{{4,5}}-[0-9]{{3,4}}-[0-9]{{1,2}}$' THEN '10-digit dashed'
        WHEN ndc RLIKE '^[0-9]+$'                THEN 'digits only, short (zeros lost?)'
        ELSE 'other / junk'
      END AS ndc_shape,
      COUNT(*) AS claim_rows,
      ANY_VALUE(ndc) AS example
    FROM {CATALOG}.bronze.claims
    GROUP BY 1
    ORDER BY claim_rows DESC
"""))
