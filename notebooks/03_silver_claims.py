# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver claims
# MAGIC
# MAGIC Turns raw bronze rows into **one row per final paid claim**, with every rule written in plain SQL.
# MAGIC
# MAGIC ```
# MAGIC bronze.claims (998k rows, all strings)
# MAGIC   │ 1. Conform     cast types, trim, normalise NDC to ndc11 ............. rows failing required fields -> rejected
# MAGIC   │ 2. Net         remove paid claims a reversal points at .............. reversals pointing at nothing -> rejected
# MAGIC   │ 3. Dedupe      one row per (rx_number, fill_date, pharmacy_npi), latest ingestion wins
# MAGIC   │ 4. Validate    NDC must map to drug_reference ....................... INVALID_NDC_FORMAT / NDC_NOT_IN_REFERENCE -> rejected
# MAGIC   │ 5. Flag        member found? eligible on fill date? pharmacy found? prescriber missing?
# MAGIC   ▼
# MAGIC silver.claims_final  +  silver.claims_rejected
# MAGIC ```
# MAGIC
# MAGIC ### Why this order
# MAGIC * **Net before dedupe.** An original, its reversal and its rebill share the same business key. Dedupe first, and those three rows
# MAGIC   collapse into one arbitrary row. Netting first removes the original. The rebill is then the only paid row left on that key.
# MAGIC * **Net before NDC validation.** A reversal of a claim with a bad NDC still has to find its original. Otherwise it looks orphaned
# MAGIC   and the bad-NDC claim survives as a false reject.
# MAGIC * **Flag eligibility rather than reject it.** An out-of-eligibility claim was still paid. Finance needs it in the spend numbers and
# MAGIC   operations needs it for recovery, so it stays in `claims_final` with a flag.
# MAGIC
# MAGIC ### Rebuild, not incremental
# MAGIC Every table is `CREATE OR REPLACE` from bronze. A reversal arriving months late can cancel a claim in any earlier month, so an
# MAGIC incremental `MERGE` would need a reprocessing window anyway. At about 1M rows a full rebuild takes a minute or two, is guaranteed correct, and is trivially
# MAGIC re-runnable. `MERGE` becomes worth it at 100M+ rows (see `docs/design_decisions.md`, D27).
# MAGIC
# MAGIC ### Outputs
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `silver.members`, `silver.pharmacies`, `silver.drug_reference`, `silver.rebate_contracts` | Latest snapshot, typed |
# MAGIC | `silver.claims_final` | One row per final paid claim with a valid NDC |
# MAGIC | `silver.claims_rejected` | One row per claim row that could not be used, with `reason_code` |

# COMMAND ----------

dbutils.widgets.text("catalog", "portfolio", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# Every SQL cell below uses unqualified schema.table names (silver.x, bronze.y), so the catalog is set once here.
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reference tables: latest snapshot, typed
# MAGIC Bronze keeps every snapshot file ever received. Silver uses the **most recent file** (by file landing time, then ingestion time),
# MAGIC so dropping in a new members file replaces the old one on the next run. `try_cast` returns NULL instead of failing the job on a bad value.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.members
# MAGIC COMMENT 'Silver: members from the latest bronze snapshot. eligibility_end_effective treats a blank end date as open-ended (9999-12-31).'
# MAGIC AS
# MAGIC SELECT
# MAGIC   trim(member_id)                                                AS member_id,
# MAGIC   trim(plan_id)                                                  AS plan_id,
# MAGIC   try_cast(birth_year AS INT)                                    AS birth_year,
# MAGIC   upper(trim(gender))                                            AS gender,
# MAGIC   upper(trim(state))                                             AS state,
# MAGIC   try_cast(eligibility_start AS DATE)                            AS eligibility_start,
# MAGIC   try_cast(eligibility_end AS DATE)                              AS eligibility_end,
# MAGIC   coalesce(try_cast(eligibility_end AS DATE), DATE'9999-12-31')  AS eligibility_end_effective,
# MAGIC   eligibility_end IS NULL                                        AS is_active,
# MAGIC   _source_file,
# MAGIC   _ingested_at
# MAGIC FROM bronze.members
# MAGIC WHERE _source_file = (SELECT max_by(_source_file, struct(_source_file_modified_at, _ingested_at)) FROM bronze.members)
# MAGIC QUALIFY row_number() OVER (PARTITION BY trim(member_id) ORDER BY try_cast(eligibility_start AS DATE) DESC) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.pharmacies
# MAGIC COMMENT 'Silver: pharmacy directory from the latest bronze snapshot. type is retail / specialty / mail.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   trim(pharmacy_npi)        AS pharmacy_npi,
# MAGIC   trim(name)                AS pharmacy_name,
# MAGIC   lower(trim(type))         AS pharmacy_type,
# MAGIC   upper(trim(state))        AS state,
# MAGIC   _source_file,
# MAGIC   _ingested_at
# MAGIC FROM bronze.pharmacies
# MAGIC WHERE _source_file = (SELECT max_by(_source_file, struct(_source_file_modified_at, _ingested_at)) FROM bronze.pharmacies)
# MAGIC QUALIFY row_number() OVER (PARTITION BY trim(pharmacy_npi) ORDER BY trim(name)) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.drug_reference
# MAGIC COMMENT 'Silver: drug reference from the latest bronze snapshot, one row per ndc11. The master list every claim NDC must map to.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   trim(ndc11)                       AS ndc11,
# MAGIC   trim(drug_name)                   AS drug_name,
# MAGIC   upper(trim(brand_generic_flag))   AS brand_generic_flag,
# MAGIC   upper(trim(specialty_flag))       AS specialty_flag,
# MAGIC   trim(therapeutic_class)           AS therapeutic_class,
# MAGIC   trim(manufacturer)                AS manufacturer,
# MAGIC   try_cast(package_size AS INT)     AS package_size,
# MAGIC   upper(trim(unit_of_measure))      AS unit_of_measure,
# MAGIC   _source_file,
# MAGIC   _ingested_at
# MAGIC FROM bronze.drug_reference
# MAGIC WHERE _source_file = (SELECT max_by(_source_file, struct(_source_file_modified_at, _ingested_at)) FROM bronze.drug_reference)
# MAGIC QUALIFY row_number() OVER (PARTITION BY trim(ndc11) ORDER BY trim(drug_name)) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.rebate_contracts
# MAGIC COMMENT 'Silver: rebate contracts, typed. contract_grain is NDC (ndc11 set) or CLASS (therapeutic_class set). rebate_pct is a percentage (22.5 = 22.5%).'
# MAGIC AS
# MAGIC SELECT
# MAGIC   trim(contract_id)                         AS contract_id,
# MAGIC   trim(manufacturer)                        AS manufacturer,
# MAGIC   nullif(trim(ndc11), '')                   AS ndc11,
# MAGIC   nullif(trim(therapeutic_class), '')       AS therapeutic_class,
# MAGIC   CASE WHEN nullif(trim(ndc11), '') IS NOT NULL THEN 'NDC' ELSE 'CLASS' END AS contract_grain,
# MAGIC   try_cast(rebate_pct AS DECIMAL(5,2))      AS rebate_pct,
# MAGIC   try_cast(effective_start AS DATE)         AS effective_start,
# MAGIC   try_cast(effective_end AS DATE)           AS effective_end,
# MAGIC   _source_file,
# MAGIC   _ingested_at
# MAGIC FROM bronze.rebate_contracts
# MAGIC WHERE _source_file = (SELECT max_by(_source_file, struct(_source_file_modified_at, _ingested_at)) FROM bronze.rebate_contracts)
# MAGIC QUALIFY row_number() OVER (PARTITION BY trim(contract_id) ORDER BY try_cast(effective_start AS DATE) DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conform claims: types and NDC normalisation
# MAGIC
# MAGIC **NDC rule:** an 11-digit NDC is 5-4-2 segments. A 10-digit NDC has one short segment (4-4-2, 5-3-2 or 5-4-1).
# MAGIC For dashed values, **left-pad each segment to 5-4-2**. That one rule handles every dashed format correctly, because the dashes
# MAGIC show which segment is short. Without dashes, a 10-digit code is ambiguous, so:
# MAGIC
# MAGIC | Source format | Example | Rule |
# MAGIC |---|---|---|
# MAGIC | `NDC11` | `07147917803` | as-is |
# MAGIC | `NDC11_DASHED` | `07147-9178-03` | remove dashes |
# MAGIC | `NDC10_DASHED` | `7702-2948-02`, `93027-754-01`, `12345-6789-0` | pad each segment to 5-4-2 |
# MAGIC | `DIGITS_ZEROS_LOST` | `7147383302` | 8–10 digits, no dashes: assume an 11-digit NDC whose leading zeros were stripped (the Excel / integer-cast failure). Left-pad to 11. |
# MAGIC | `INVALID` | null, `UNKNOWN`, `12345` | cannot be an NDC -> rejected |
# MAGIC
# MAGIC The zeros-lost assumption is a guess, and it is **checked by the reference lookup** in step 5. If the padded code isn't a real NDC,
# MAGIC the claim is rejected as `NDC_NOT_IN_REFERENCE` instead of being silently mis-mapped.
# MAGIC
# MAGIC These are temporary views: intermediate steps that only exist for this run. Only the final outputs are written as tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW claims_conformed AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     trim(claim_id)                                  AS claim_id,
# MAGIC     trim(rx_number)                                 AS rx_number,
# MAGIC     try_cast(fill_date AS DATE)                     AS fill_date,
# MAGIC     fill_date                                       AS fill_date_raw,
# MAGIC     trim(member_id)                                 AS member_id,
# MAGIC     trim(pharmacy_npi)                              AS pharmacy_npi,
# MAGIC     nullif(trim(prescriber_npi), '')                AS prescriber_npi,
# MAGIC     trim(ndc)                                       AS ndc_raw,
# MAGIC     try_cast(quantity_dispensed AS DECIMAL(12,3))   AS quantity_dispensed,
# MAGIC     try_cast(days_supply AS INT)                    AS days_supply,
# MAGIC     try_cast(ingredient_cost AS DECIMAL(12,2))      AS ingredient_cost,
# MAGIC     try_cast(dispensing_fee AS DECIMAL(12,2))       AS dispensing_fee,
# MAGIC     try_cast(copay AS DECIMAL(12,2))                AS copay,
# MAGIC     try_cast(plan_paid AS DECIMAL(12,2))            AS plan_paid,
# MAGIC     lower(trim(claim_status))                       AS claim_status,
# MAGIC     nullif(trim(reversal_of_claim_id), '')          AS reversal_of_claim_id,
# MAGIC     _source_file,
# MAGIC     _source_file_modified_at,
# MAGIC     _ingested_at
# MAGIC   FROM bronze.claims
# MAGIC ),
# MAGIC formatted AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN ndc_raw RLIKE '^[0-9]{11}$'                          THEN 'NDC11'
# MAGIC       WHEN ndc_raw RLIKE '^[0-9]{5}-[0-9]{4}-[0-9]{2}$'         THEN 'NDC11_DASHED'
# MAGIC       WHEN ndc_raw RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{2}$'
# MAGIC         OR ndc_raw RLIKE '^[0-9]{5}-[0-9]{3}-[0-9]{2}$'
# MAGIC         OR ndc_raw RLIKE '^[0-9]{5}-[0-9]{4}-[0-9]{1}$'         THEN 'NDC10_DASHED'
# MAGIC       WHEN ndc_raw RLIKE '^[0-9]{8,10}$'                        THEN 'DIGITS_ZEROS_LOST'
# MAGIC       ELSE 'INVALID'
# MAGIC     END AS ndc_source_format
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN ndc_source_format = 'NDC11'             THEN ndc_raw
# MAGIC     WHEN ndc_source_format IN ('NDC11_DASHED', 'NDC10_DASHED')
# MAGIC       THEN concat(lpad(split(ndc_raw, '-')[0], 5, '0'),
# MAGIC                   lpad(split(ndc_raw, '-')[1], 4, '0'),
# MAGIC                   lpad(split(ndc_raw, '-')[2], 2, '0'))
# MAGIC     WHEN ndc_source_format = 'DIGITS_ZEROS_LOST' THEN lpad(ndc_raw, 11, '0')
# MAGIC   END AS ndc11,
# MAGIC   -- A row is usable only if every field the later rules depend on is present and typed correctly
# MAGIC   (claim_id IS NOT NULL AND rx_number IS NOT NULL AND fill_date IS NOT NULL AND pharmacy_npi IS NOT NULL
# MAGIC    AND member_id IS NOT NULL AND claim_status IN ('paid', 'reversed')
# MAGIC    AND ingredient_cost IS NOT NULL AND dispensing_fee IS NOT NULL AND copay IS NOT NULL AND plan_paid IS NOT NULL
# MAGIC    AND (claim_status = 'paid' OR reversal_of_claim_id IS NOT NULL)) AS has_required_fields
# MAGIC FROM formatted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Net reversals
# MAGIC A paid claim is cancelled if **any** reversal points at its `claim_id`. This is an anti-join, so it doesn't matter which file
# MAGIC arrived first. About 1,100 reversals in this data arrive *before* the claim they cancel.
# MAGIC An exact duplicate of a reversed claim has the same `claim_id`, so both copies are cancelled together.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW reversals AS
# MAGIC SELECT * FROM claims_conformed WHERE claim_status = 'reversed' AND has_required_fields;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW paid_after_netting AS
# MAGIC SELECT p.*
# MAGIC FROM claims_conformed p
# MAGIC LEFT ANTI JOIN reversals r ON r.reversal_of_claim_id = p.claim_id
# MAGIC WHERE p.claim_status = 'paid' AND p.has_required_fields;
# MAGIC
# MAGIC -- Reversals whose target claim_id was never received: nothing to cancel, so they are rejected for investigation
# MAGIC CREATE OR REPLACE TEMP VIEW orphan_reversals AS
# MAGIC SELECT r.*
# MAGIC FROM reversals r
# MAGIC LEFT ANTI JOIN (SELECT DISTINCT claim_id FROM claims_conformed WHERE claim_status = 'paid') p
# MAGIC   ON r.reversal_of_claim_id = p.claim_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicate on the business key
# MAGIC One row per `(rx_number, fill_date, pharmacy_npi)`. **Latest ingestion wins**, ordered by `_ingested_at`, then `_source_file_modified_at`
# MAGIC (when files load in the same run they share an `_ingested_at`), then `_source_file`, then `claim_id` as a final deterministic tie-breaker.
# MAGIC This catches:
# MAGIC * exact double-writes in the same file,
# MAGIC * resends in the late file, including ones with a **new** `claim_id` (only the business key catches those) and ones with a
# MAGIC   **corrected copay** (the later version wins).
# MAGIC
# MAGIC `is_rebill` marks a surviving claim whose key also had a reversal, i.e. a corrected resubmission.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW paid_deduped AS
# MAGIC SELECT
# MAGIC   p.*,
# MAGIC   count(*) OVER (PARTITION BY p.rx_number, p.fill_date, p.pharmacy_npi) - 1 AS duplicates_removed
# MAGIC FROM paid_after_netting p
# MAGIC QUALIFY row_number() OVER (
# MAGIC   PARTITION BY p.rx_number, p.fill_date, p.pharmacy_npi
# MAGIC   ORDER BY p._ingested_at DESC, p._source_file_modified_at DESC, p._source_file DESC, p.claim_id DESC
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write `silver.claims_final`
# MAGIC Only claims whose `ndc11` exists in `silver.drug_reference`. Eligibility and reference problems are **flags**, not rejects.
# MAGIC `gross_cost = ingredient_cost + dispensing_fee`, which is the claim's total allowed amount before the member's share.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.claims_final
# MAGIC COMMENT 'Silver: one row per final paid claim. Reversals netted, deduplicated on (rx_number, fill_date, pharmacy_npi) with latest ingestion winning, NDC normalised to ndc11 and present in drug_reference. Eligibility and reference issues are flagged, not removed.'
# MAGIC AS
# MAGIC WITH reversed_keys AS (
# MAGIC   SELECT DISTINCT rx_number, fill_date, pharmacy_npi FROM reversals
# MAGIC )
# MAGIC SELECT
# MAGIC   c.claim_id,
# MAGIC   c.rx_number,
# MAGIC   c.fill_date,
# MAGIC   CAST(date_trunc('MONTH', c.fill_date) AS DATE)          AS fill_month,
# MAGIC   c.member_id,
# MAGIC   c.pharmacy_npi,
# MAGIC   c.prescriber_npi,
# MAGIC   c.ndc11,
# MAGIC   c.ndc_raw,
# MAGIC   c.ndc_source_format,
# MAGIC   c.quantity_dispensed,
# MAGIC   c.days_supply,
# MAGIC   c.ingredient_cost,
# MAGIC   c.dispensing_fee,
# MAGIC   c.ingredient_cost + c.dispensing_fee                   AS gross_cost,
# MAGIC   c.copay,
# MAGIC   c.plan_paid,
# MAGIC   -- Flags
# MAGIC   m.member_id IS NOT NULL                                AS member_found,
# MAGIC   coalesce(c.fill_date BETWEEN m.eligibility_start AND m.eligibility_end_effective, false) AS is_eligible_on_fill_date,
# MAGIC   ph.pharmacy_npi IS NOT NULL                            AS pharmacy_found,
# MAGIC   c.prescriber_npi IS NULL                               AS prescriber_missing,
# MAGIC   rk.rx_number IS NOT NULL                               AS is_rebill,
# MAGIC   c.duplicates_removed,
# MAGIC   -- Lineage
# MAGIC   c._source_file,
# MAGIC   c._source_file_modified_at,
# MAGIC   c._ingested_at,
# MAGIC   current_timestamp()                                    AS _silver_processed_at
# MAGIC FROM paid_deduped c
# MAGIC JOIN silver.drug_reference d  ON d.ndc11 = c.ndc11
# MAGIC LEFT JOIN silver.members m    ON m.member_id = c.member_id
# MAGIC LEFT JOIN silver.pharmacies ph ON ph.pharmacy_npi = c.pharmacy_npi
# MAGIC LEFT JOIN reversed_keys rk    ON rk.rx_number = c.rx_number AND rk.fill_date = c.fill_date AND rk.pharmacy_npi = c.pharmacy_npi;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write `silver.claims_rejected`
# MAGIC Every claim row that could not be used, with a reason. Rows removed by netting or dedupe are **not** here, because they were handled
# MAGIC correctly, not rejected. `plan_paid` is kept so gold can size the money behind each reason (for example, rebates that can't be claimed on unmapped NDCs).
# MAGIC
# MAGIC | reason_code | Stage | Meaning |
# MAGIC |---|---|---|
# MAGIC | `MISSING_REQUIRED_FIELD` | Conform | A key, date, status or amount is missing or not castable |
# MAGIC | `ORPHAN_REVERSAL` | Net | Reversal of a `claim_id` that was never received |
# MAGIC | `INVALID_NDC_FORMAT` | Validate | NDC can't be an NDC (null, junk, wrong length) |
# MAGIC | `NDC_NOT_IN_REFERENCE` | Validate | Well-formed NDC missing from `drug_reference` (often a new launch the reference file hasn't caught up with) |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.claims_rejected
# MAGIC COMMENT 'Silver: claim rows that could not be used, one row per rejected bronze row, with reason_code. Netted and deduplicated rows are not rejects.'
# MAGIC AS
# MAGIC WITH rejects AS (
# MAGIC   SELECT *, 'MISSING_REQUIRED_FIELD' AS reason_code,
# MAGIC          'Key, date, status or amount missing or not castable' AS reason_detail
# MAGIC   FROM claims_conformed WHERE NOT has_required_fields
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT *, 'ORPHAN_REVERSAL',
# MAGIC          concat('Reverses ', reversal_of_claim_id, ', which was never received')
# MAGIC   FROM orphan_reversals
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT * EXCEPT (duplicates_removed), 'INVALID_NDC_FORMAT',
# MAGIC          concat('Cannot parse NDC: ', coalesce(ndc_raw, '<null>'))
# MAGIC   FROM paid_deduped WHERE ndc_source_format = 'INVALID'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT * EXCEPT (duplicates_removed), 'NDC_NOT_IN_REFERENCE',
# MAGIC          concat('Normalised to ', ndc11, ' but not in drug_reference')
# MAGIC   FROM paid_deduped p
# MAGIC   WHERE ndc_source_format <> 'INVALID'
# MAGIC     AND NOT EXISTS (SELECT 1 FROM silver.drug_reference d WHERE d.ndc11 = p.ndc11)
# MAGIC )
# MAGIC SELECT
# MAGIC   reason_code,
# MAGIC   reason_detail,
# MAGIC   claim_id,
# MAGIC   rx_number,
# MAGIC   fill_date_raw          AS fill_date,
# MAGIC   member_id,
# MAGIC   pharmacy_npi,
# MAGIC   ndc_raw,
# MAGIC   ndc11                  AS ndc11_attempted,
# MAGIC   ndc_source_format,
# MAGIC   claim_status,
# MAGIC   reversal_of_claim_id,
# MAGIC   plan_paid,
# MAGIC   _source_file,
# MAGIC   _ingested_at,
# MAGIC   current_timestamp()    AS _silver_processed_at
# MAGIC FROM rejects;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reconcile against the generator's answer key
# MAGIC A waterfall from bronze rows to final claims. Every step must add up, and the key totals must equal the manifest.

# COMMAND ----------

import json

from pyspark.sql import functions as F

with open(f"/Volumes/{CATALOG}/raw/landing/_manifest/generation_manifest.json") as fh:
    manifest = json.load(fh)


def n(sql: str) -> int:
    return spark.sql(sql).first()[0]


bronze_rows = n("SELECT count(*) FROM bronze.claims")
missing_req = n("SELECT count(*) FROM claims_conformed WHERE NOT has_required_fields")
reversal_rows = n("SELECT count(*) FROM reversals")
orphans = n("SELECT count(*) FROM orphan_reversals")
paid_rows = n("SELECT count(*) FROM claims_conformed WHERE claim_status = 'paid' AND has_required_fields")
paid_netted = n("SELECT count(*) FROM paid_after_netting")
paid_dedup = n("SELECT count(*) FROM paid_deduped")
rej = {r.reason_code: r.cnt for r in spark.sql("SELECT reason_code, count(*) AS cnt FROM silver.claims_rejected GROUP BY 1").collect()}
final_rows = n("SELECT count(*) FROM silver.claims_final")

waterfall = [
    ("1. Bronze claim rows", bronze_rows),
    ("   - missing required fields (rejected)", -missing_req),
    ("   - reversal rows (used for netting, or rejected as orphan)", -reversal_rows),
    ("= Paid rows", paid_rows),
    ("   - paid rows cancelled by a reversal", -(paid_rows - paid_netted)),
    ("   - duplicates removed (business key)", -(paid_netted - paid_dedup)),
    ("= Final paid claims before NDC check", paid_dedup),
    ("   - INVALID_NDC_FORMAT (rejected)", -rej.get("INVALID_NDC_FORMAT", 0)),
    ("   - NDC_NOT_IN_REFERENCE (rejected)", -rej.get("NDC_NOT_IN_REFERENCE", 0)),
    ("= silver.claims_final", final_rows),
]
display(spark.createDataFrame(waterfall, "step string, rows long"))

checks = [
    ("Bronze rows add up to paid + reversal + missing", bronze_rows == paid_rows + reversal_rows + missing_req),
    ("Final before NDC check = manifest answer key",
     paid_dedup == manifest["expected_after_silver"]["final_paid_claims_before_ndc_rejects"]),
    ("Orphan reversals = manifest", orphans == manifest["row_counts"]["orphan_reversals"]),
    ("Final + NDC rejects = final before NDC check",
     final_rows + rej.get("INVALID_NDC_FORMAT", 0) + rej.get("NDC_NOT_IN_REFERENCE", 0) == paid_dedup),
    ("claims_final has one row per business key",
     n("SELECT count(*) - count(DISTINCT rx_number, fill_date, pharmacy_npi) FROM silver.claims_final") == 0),
    ("claims_final has unique claim_id", n("SELECT count(*) - count(DISTINCT claim_id) FROM silver.claims_final") == 0),
]
display(spark.createDataFrame([(c, "PASS" if ok else "FAIL") for c, ok in checks], "check string, result string"))
assert all(ok for _, ok in checks), "Silver reconciliation failed. See the checks table above."

# COMMAND ----------

# MAGIC %md ## 8. Look at the results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Flags on final claims
# MAGIC SELECT
# MAGIC   count(*)                                                        AS final_claims,
# MAGIC   sum(CASE WHEN NOT member_found THEN 1 ELSE 0 END)               AS unknown_member,
# MAGIC   sum(CASE WHEN member_found AND NOT is_eligible_on_fill_date THEN 1 ELSE 0 END) AS outside_eligibility,
# MAGIC   sum(CASE WHEN NOT pharmacy_found THEN 1 ELSE 0 END)             AS unknown_pharmacy,
# MAGIC   sum(CASE WHEN prescriber_missing THEN 1 ELSE 0 END)             AS prescriber_missing,
# MAGIC   sum(CASE WHEN is_rebill THEN 1 ELSE 0 END)                      AS rebills,
# MAGIC   round(sum(gross_cost) / 1e6, 1)                                 AS gross_cost_musd
# MAGIC FROM silver.claims_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How each NDC arrived, and where it ended up
# MAGIC SELECT ndc_source_format, 'claims_final' AS landed_in, count(*) AS claims FROM silver.claims_final GROUP BY 1
# MAGIC UNION ALL
# MAGIC SELECT ndc_source_format, concat('rejected: ', reason_code), count(*) FROM silver.claims_rejected GROUP BY 1, 2
# MAGIC ORDER BY claims DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rejects by reason, with the plan-paid dollars behind them
# MAGIC SELECT reason_code, count(*) AS claim_rows, round(sum(plan_paid), 2) AS plan_paid, any_value(reason_detail) AS example
# MAGIC FROM silver.claims_rejected
# MAGIC GROUP BY reason_code
# MAGIC ORDER BY claim_rows DESC;
