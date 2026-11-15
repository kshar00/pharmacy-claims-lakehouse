# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver data quality checks
# MAGIC
# MAGIC Runs a list of data quality checks over bronze and silver and **appends** one result row per check to `silver.dq_results`.
# MAGIC
# MAGIC ### How a check works
# MAGIC Each check is one line in the `CHECKS` list below: a table, an **expectation** (SQL that should be true for every row), a severity,
# MAGIC and a tolerance. The notebook counts the rows that break the expectation:
# MAGIC
# MAGIC | Result | When |
# MAGIC |---|---|
# MAGIC | `PASS` | failed rows % ≤ tolerance |
# MAGIC | `WARN` | over tolerance, severity `WARN`. Recorded, the job carries on |
# MAGIC | `FAIL` | over tolerance, severity `ERROR`. Recorded, then **the job stops** |
# MAGIC
# MAGIC * **`ERROR` with 0% tolerance** is for things silver guarantees (unique keys, no null amounts). If one breaks, the pipeline has a bug
# MAGIC   and gold must not be built on it.
# MAGIC * **`WARN` with a tolerance** is for expected source messiness (unknown members, eligibility gaps). A small amount is normal.
# MAGIC   A jump above tolerance is a signal for the data owner, not a reason to stop the business getting its numbers.
# MAGIC * A NULL expectation counts as a **failure**: "unknown" is not "fine".
# MAGIC
# MAGIC Results are written **before** the job is failed, so even a failed run leaves a record of what broke.
# MAGIC
# MAGIC ### Check categories
# MAGIC `completeness` (row counts, answer key) · `uniqueness` · `nulls` · `referential` · `validity`

# COMMAND ----------

dbutils.widgets.text("catalog", "portfolio", "Catalog")
dbutils.widgets.text("run_id", "manual", "Run id")

import json
from datetime import datetime, timezone

CATALOG = dbutils.widgets.get("catalog")
RUN_TS = datetime.now(timezone.utc)
RUN_ID = dbutils.widgets.get("run_id")
if RUN_ID == "manual":
    RUN_ID = f"manual-{RUN_TS:%Y%m%dT%H%M%S}"  # every manual run gets its own id in the history

spark.sql(f"USE CATALOG {CATALOG}")
print(f"catalog={CATALOG} | run_id={RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable NPI checksum function
# MAGIC Real NPIs end in a Luhn check digit computed over `80840` + the first 9 digits (CMS standard).
# MAGIC It is stored as a **SQL function in Unity Catalog** (`silver.is_valid_npi`), so the dashboard and ad-hoc queries can use the same rule.
# MAGIC
# MAGIC How it reads: take the 15 characters `'80840' || npi`. Walking from the right, double every second digit (subtract 9 if over 9).
# MAGIC Add everything up. The NPI is valid if the total ends in 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION silver.is_valid_npi(npi STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'TRUE if npi is 10 digits and passes the CMS Luhn check digit (prefix 80840).'
# MAGIC RETURN
# MAGIC   CASE WHEN npi RLIKE '^[0-9]{10}$' THEN
# MAGIC     aggregate(
# MAGIC       sequence(1, 15), 0,
# MAGIC       (total, p) -> total + CASE
# MAGIC         WHEN (15 - p) % 2 = 1 THEN  -- every second digit from the right is doubled
# MAGIC           CASE WHEN CAST(substr(concat('80840', npi), p, 1) AS INT) * 2 > 9
# MAGIC                THEN CAST(substr(concat('80840', npi), p, 1) AS INT) * 2 - 9
# MAGIC                ELSE CAST(substr(concat('80840', npi), p, 1) AS INT) * 2 END
# MAGIC         ELSE CAST(substr(concat('80840', npi), p, 1) AS INT)
# MAGIC       END
# MAGIC     ) % 10 = 0
# MAGIC   ELSE FALSE END;

# COMMAND ----------

# Sanity check on a known-valid and a known-invalid NPI before trusting the function in checks
assert spark.sql("SELECT silver.is_valid_npi('1234567893') AND NOT silver.is_valid_npi('1234567890')").first()[0], \
    "is_valid_npi returned the wrong answer for the reference NPIs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The checks
# MAGIC Two ways to write a check:
# MAGIC * `expect` – a condition every row of `table` should meet (optionally filtered by `where`). This covers most checks.
# MAGIC * `sql` – a custom query returning `failed, total`, for checks that compare tables or counts.
# MAGIC
# MAGIC `max_failed_pct` is the tolerance as a percentage of `total`.

# COMMAND ----------

with open(f"/Volumes/{CATALOG}/raw/landing/_manifest/generation_manifest.json") as fh:
    manifest = json.load(fh)
EXPECTED_BEFORE_NDC = manifest["expected_after_silver"]["final_paid_claims_before_ndc_rejects"]

CHECKS = []


def check(check_id, category, table, name, expect=None, where=None, sql=None, severity="ERROR", max_failed_pct=0.0):
    if sql is None:
        # NOT coalesce(expect, false): a row fails when the expectation is false OR NULL
        sql = (f"SELECT count_if(NOT coalesce({expect}, false)) AS failed, count(*) AS total "
               f"FROM {table}" + (f" WHERE {where}" if where else ""))
    CHECKS.append(dict(check_id=check_id, category=category, table_name=table, check_name=name,
                       severity=severity, max_failed_pct=float(max_failed_pct), sql=sql))


NDC_REJECTS = "reason_code IN ('INVALID_NDC_FORMAT', 'NDC_NOT_IN_REFERENCE')"

# ---- Completeness: tables are populated and silver reconciles to the answer key ----
check("CP01", "completeness", "bronze.claims", "Bronze claims table is not empty",
      sql="SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END, 1 FROM bronze.claims")
for i, t in enumerate(["members", "pharmacies", "drug_reference", "rebate_contracts"], start=2):
    check(f"CP0{i}", "completeness", f"silver.{t}", f"silver.{t} is not empty",
          sql=f"SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END, 1 FROM silver.{t}")
check("CP06", "completeness", "silver.claims_final",
      f"claims_final + NDC rejects = answer key ({EXPECTED_BEFORE_NDC:,})",
      sql=f"""SELECT CASE WHEN (SELECT count(*) FROM silver.claims_final)
                             + (SELECT count(*) FROM silver.claims_rejected WHERE {NDC_REJECTS}) = {EXPECTED_BEFORE_NDC}
                          THEN 0 ELSE 1 END, 1""")

# ---- Uniqueness: one row per key ----
for cid, table, key, label in [
    ("UQ01", "silver.claims_final", "rx_number, fill_date, pharmacy_npi", "Business key (rx_number, fill_date, pharmacy_npi) is unique"),
    ("UQ02", "silver.claims_final", "claim_id", "claim_id is unique"),
    ("UQ03", "silver.members", "member_id", "member_id is unique"),
    ("UQ04", "silver.pharmacies", "pharmacy_npi", "pharmacy_npi is unique"),
    ("UQ05", "silver.drug_reference", "ndc11", "ndc11 is unique"),
]:
    check(cid, "uniqueness", table, label, sql=f"SELECT count(*) - count(DISTINCT {key}), count(*) FROM {table}")

# ---- Nulls: required fields populated (try_cast in silver turns bad values into NULLs, and these catch them) ----
check("NL01", "nulls", "silver.claims_final", "Required claim fields populated",
      expect="""claim_id IS NOT NULL AND rx_number IS NOT NULL AND fill_date IS NOT NULL AND member_id IS NOT NULL
                AND pharmacy_npi IS NOT NULL AND ndc11 IS NOT NULL AND quantity_dispensed IS NOT NULL AND days_supply IS NOT NULL
                AND ingredient_cost IS NOT NULL AND dispensing_fee IS NOT NULL AND copay IS NOT NULL AND plan_paid IS NOT NULL""")
check("NL02", "nulls", "silver.claims_final", "Prescriber NPI present",
      expect="prescriber_npi IS NOT NULL", severity="WARN", max_failed_pct=1.0)
check("NL03", "nulls", "silver.members", "Member plan, birth year and eligibility start populated",
      expect="plan_id IS NOT NULL AND birth_year IS NOT NULL AND eligibility_start IS NOT NULL")
check("NL04", "nulls", "silver.drug_reference", "Drug attributes populated and correctly coded",
      expect="""drug_name IS NOT NULL AND therapeutic_class IS NOT NULL AND manufacturer IS NOT NULL AND package_size IS NOT NULL
                AND brand_generic_flag IN ('B', 'G') AND specialty_flag IN ('Y', 'N')""")
check("NL05", "nulls", "silver.rebate_contracts", "Contract terms populated",
      expect="manufacturer IS NOT NULL AND rebate_pct IS NOT NULL AND effective_start IS NOT NULL AND effective_end IS NOT NULL")

# ---- Referential: keys point at something real ----
check("RI01", "referential", "silver.claims_final", "Every final claim's ndc11 is in drug_reference",
      sql="""SELECT count_if(d.ndc11 IS NULL), count(*)
             FROM silver.claims_final c LEFT JOIN silver.drug_reference d ON d.ndc11 = c.ndc11""")
check("RI02", "referential", "silver.claims_final", "Member is in the member file",
      expect="member_found", severity="WARN", max_failed_pct=0.5)
check("RI03", "referential", "silver.claims_final", "Pharmacy is in the pharmacy directory",
      expect="pharmacy_found", severity="WARN", max_failed_pct=0.5)
check("RI04", "referential", "silver.claims_final", "Member eligible on fill date (known members)",
      expect="is_eligible_on_fill_date", where="member_found", severity="WARN", max_failed_pct=1.0)
check("RI05", "referential", "silver.claims_rejected", "NDC reject rate (unparseable or unmapped NDC)",
      sql=f"""SELECT count_if({NDC_REJECTS}),
                     count_if({NDC_REJECTS}) + (SELECT count(*) FROM silver.claims_final)
              FROM silver.claims_rejected""", severity="WARN", max_failed_pct=2.0)
check("RI06", "referential", "silver.claims_rejected", "Orphan reversal rate (reverses a claim never received)",
      sql="""SELECT (SELECT count(*) FROM silver.claims_rejected WHERE reason_code = 'ORPHAN_REVERSAL'), count(*)
             FROM bronze.claims WHERE lower(trim(claim_status)) = 'reversed'""", severity="WARN", max_failed_pct=1.0)
check("RI07", "referential", "silver.rebate_contracts", "NDC-level contract ndc11 is in drug_reference",
      sql="""SELECT count_if(d.ndc11 IS NULL), count(*)
             FROM silver.rebate_contracts c LEFT JOIN silver.drug_reference d ON d.ndc11 = c.ndc11
             WHERE c.contract_grain = 'NDC'""")
check("RI08", "referential", "silver.rebate_contracts", "Class-level contract therapeutic_class exists in drug_reference",
      sql="""SELECT count_if(d.therapeutic_class IS NULL), count(*)
             FROM silver.rebate_contracts c
             LEFT JOIN (SELECT DISTINCT therapeutic_class FROM silver.drug_reference) d ON d.therapeutic_class = c.therapeutic_class
             WHERE c.contract_grain = 'CLASS'""")

# ---- Validity: values make business sense ----
check("VA01", "validity", "silver.claims_final", "Amounts are not negative",
      expect="ingredient_cost >= 0 AND dispensing_fee >= 0 AND copay >= 0 AND plan_paid >= 0")
check("VA02", "validity", "silver.claims_final", "plan_paid = ingredient_cost + dispensing_fee - copay (to the cent)",
      expect="abs(ingredient_cost + dispensing_fee - copay - plan_paid) <= 0.01")
check("VA03", "validity", "silver.claims_final", "days_supply 1-365 and quantity > 0",
      expect="days_supply BETWEEN 1 AND 365 AND quantity_dispensed > 0")
check("VA04", "validity", "silver.claims_final", "fill_date between 2000-01-01 and today",
      expect="fill_date BETWEEN DATE'2000-01-01' AND current_date()")
check("VA05", "validity", "silver.pharmacies", "Pharmacy directory NPIs pass the CMS checksum",
      expect="silver.is_valid_npi(pharmacy_npi)")
check("VA06", "validity", "silver.claims_final", "Claim pharmacy NPI passes the CMS checksum",
      expect="silver.is_valid_npi(pharmacy_npi)", severity="WARN", max_failed_pct=0.5)
check("VA07", "validity", "silver.claims_final", "Claim prescriber NPI passes the CMS checksum (when present)",
      expect="silver.is_valid_npi(prescriber_npi)", where="prescriber_npi IS NOT NULL", severity="WARN", max_failed_pct=0.5)
check("VA08", "validity", "silver.members", "Eligibility end is on or after eligibility start",
      expect="eligibility_end_effective >= eligibility_start")
check("VA09", "validity", "silver.rebate_contracts", "Contract dates ordered, 0 < rebate_pct <= 100, exactly one of ndc11 / class",
      expect="""effective_end >= effective_start AND rebate_pct > 0 AND rebate_pct <= 100
                AND ((ndc11 IS NULL) <> (therapeutic_class IS NULL))""")
check("VA10", "validity", "silver.rebate_contracts", "No overlapping contracts for the same manufacturer + NDC/class",
      sql="""SELECT count(DISTINCT a.contract_id), (SELECT count(*) FROM silver.rebate_contracts)
             FROM silver.rebate_contracts a
             JOIN silver.rebate_contracts b
               ON  a.contract_id <> b.contract_id
               AND a.manufacturer = b.manufacturer
               AND a.contract_grain = b.contract_grain
               AND coalesce(a.ndc11, a.therapeutic_class) = coalesce(b.ndc11, b.therapeutic_class)
               AND a.effective_start <= b.effective_end
               AND b.effective_start <= a.effective_end""")

print(f"{len(CHECKS)} checks defined")

# COMMAND ----------

# MAGIC %md ## Run the checks

# COMMAND ----------

results = []
for c in CHECKS:
    row = spark.sql(c["sql"]).first()
    failed, total = int(row[0] or 0), int(row[1] or 0)
    failed_pct = round(100.0 * failed / total, 4) if total else 0.0
    if failed_pct <= c["max_failed_pct"]:
        status = "PASS"
    else:
        status = "FAIL" if c["severity"] == "ERROR" else "WARN"
    results.append((
        RUN_ID, RUN_TS, c["check_id"], c["category"], c["table_name"], c["check_name"], c["severity"],
        failed, total, failed_pct, c["max_failed_pct"], status, " ".join(c["sql"].split()),
    ))

RESULT_SCHEMA = """
    run_id STRING, run_ts TIMESTAMP, check_id STRING, category STRING, table_name STRING, check_name STRING,
    severity STRING, failed_rows BIGINT, total_rows BIGINT, failed_pct DOUBLE, max_failed_pct DOUBLE,
    status STRING, check_sql STRING
"""
results_df = spark.createDataFrame(results, RESULT_SCHEMA)
display(results_df.drop("run_id", "run_ts", "check_sql").orderBy("check_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to `silver.dq_results` (append, never replace)
# MAGIC Unlike the other silver tables, this one is **appended**: each run adds its rows, which gives a history of data quality over time
# MAGIC (for example, "unknown member rate by week"). `silver.dq_results_latest` is a view of the most recent run, for the dashboard.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.dq_results ({RESULT_SCHEMA})
    COMMENT 'Silver: data quality check results, one row per check per run. Appended every run, so it holds the full history.'
""")
results_df.write.mode("append").saveAsTable("silver.dq_results")

spark.sql("""
    CREATE OR REPLACE VIEW silver.dq_results_latest
    COMMENT 'Data quality results from the most recent run only.'
    AS SELECT * FROM silver.dq_results
    WHERE run_ts = (SELECT max(run_ts) FROM silver.dq_results)
""")
print(f"Saved {len(results)} results for run {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## Summary, and stop the job on any ERROR failure

# COMMAND ----------

display(spark.sql("""
    SELECT status, count(*) AS checks, concat_ws(', ', sort_array(collect_list(check_id))) AS check_ids
    FROM silver.dq_results_latest GROUP BY status ORDER BY status
"""))

failures = [r for r in results if r[11] == "FAIL"]
warnings = [r for r in results if r[11] == "WARN"]
for r in warnings:
    print(f"WARN  {r[2]} {r[5]}: {r[7]:,} of {r[8]:,} rows ({r[9]:.2f}% > {r[10]}% tolerance)")
if failures:
    raise Exception("Data quality FAILED (results saved to silver.dq_results): "
                    + "; ".join(f"{r[2]} {r[5]} ({r[7]:,} rows)" for r in failures))
print(f"Data quality OK: {len(results) - len(warnings)} PASS, {len(warnings)} WARN, 0 FAIL")
