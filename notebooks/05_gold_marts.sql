-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Gold marts
-- MAGIC
-- MAGIC Business-ready tables built from `silver.claims_final` and the silver reference tables. All plain SQL.
-- MAGIC
-- MAGIC | Object | Grain | Answers |
-- MAGIC |---|---|---|
-- MAGIC | `gold.drug_spend_monthly` | month × drug | Where is the money going? Specialty vs traditional, brand vs generic, by class |
-- MAGIC | `gold.utilisation` | month × plan | How much do members use? Claims per 1,000 members (annualised), PMPM cost, days supply |
-- MAGIC | `gold.top_drugs` (view) | year × drug | Top drugs by spend and by claim count |
-- MAGIC | `gold.rebate_eligible_claims` | claim | Which brand claims earn a rebate, under which contract, worth how much |
-- MAGIC | `gold.pipeline_audit` | run × table | Did every layer reconcile? Row counts and dollars per layer per run |
-- MAGIC
-- MAGIC **Rebuilt every run** with `CREATE OR REPLACE`, apart from `pipeline_audit`, which keeps history and is upserted with `MERGE`.
-- MAGIC The notebook ends with reconciliation checks that **fail the job** if gold totals don't match silver.
-- MAGIC
-- MAGIC Uses the `portfolio` catalog. If you set up a different catalog in notebook 00, change it in the next cell and in the
-- MAGIC `read_files` path in the pipeline audit section.

-- COMMAND ----------

USE CATALOG portfolio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. `gold.drug_spend_monthly`
-- MAGIC One row per fill month and drug (a drug can have several NDC package sizes; they are rolled up by `drug_name`).
-- MAGIC
-- MAGIC Spend measures, from the plan's point of view:
-- MAGIC * `gross_cost` = ingredient cost + dispensing fee, the total cost of the claim
-- MAGIC * `member_copay` = what the member paid
-- MAGIC * `plan_paid` = gross_cost − member_copay, what the plan paid
-- MAGIC
-- MAGIC Out-of-eligibility claims are **included** (they were paid) and also broken out, so the dashboard can show the money at risk.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.drug_spend_monthly
COMMENT 'Gold: spend and volume by fill month and drug. All final paid claims, with out-of-eligibility claims also broken out.'
AS
SELECT
  c.fill_month,
  d.drug_name,
  d.therapeutic_class,
  d.specialty_flag,
  d.brand_generic_flag,
  d.manufacturer,
  count(*)                                                     AS claim_count,
  count(DISTINCT c.member_id)                                  AS unique_members,
  sum(c.quantity_dispensed)                                    AS total_quantity,
  sum(c.days_supply)                                           AS total_days_supply,
  sum(c.ingredient_cost)                                       AS ingredient_cost,
  sum(c.dispensing_fee)                                        AS dispensing_fee,
  sum(c.gross_cost)                                            AS gross_cost,
  sum(c.copay)                                                 AS member_copay,
  sum(c.plan_paid)                                             AS plan_paid,
  count_if(c.member_found AND NOT c.is_eligible_on_fill_date)  AS claims_outside_eligibility,
  sum(CASE WHEN c.member_found AND NOT c.is_eligible_on_fill_date THEN c.plan_paid ELSE 0 END) AS plan_paid_outside_eligibility,
  count_if(NOT c.member_found)                                 AS claims_unknown_member
FROM silver.claims_final c
JOIN silver.drug_reference d ON d.ndc11 = c.ndc11
GROUP BY ALL;  -- groups by every non-aggregated column above

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. `gold.utilisation`
-- MAGIC **The denominator matters.** "Claims per 1,000 members" divided by a simple member count would be wrong, because members join and leave
-- MAGIC during the period. The standard is **eligible member-months**: each member counts once for every month they're eligible.
-- MAGIC Eligibility in this data starts on the 1st and ends at month-end, so "eligible in a month" means the eligibility window overlaps that month.
-- MAGIC
-- MAGIC | Metric | Formula |
-- MAGIC |---|---|
-- MAGIC | `claims_per_1000_members_annualised` | claims ÷ member_months × 12 × 1,000 (the usual "scripts per 1,000 per year") |
-- MAGIC | `adjusted_scripts_per_1000_annualised` | Same, counting a 90-day fill as 3 scripts (days_supply ÷ 30), so mail and retail compare fairly |
-- MAGIC | `gross_cost_pmpm` | gross cost ÷ member_months |
-- MAGIC | `specialty_spend_share` | specialty gross cost ÷ all gross cost |
-- MAGIC
-- MAGIC Claims for members missing from the member file can't be given a plan or a denominator. They get `plan_id = 'UNKNOWN_MEMBER'` with
-- MAGIC null rates, so claim totals still reconcile to silver.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.utilisation
COMMENT 'Gold: utilisation by fill month and plan, normalised by eligible member-months. UNKNOWN_MEMBER holds claims whose member is not in the member file.'
AS
WITH months AS (  -- one row per calendar month in the claim period
  SELECT explode(sequence(first_month, last_month, INTERVAL 1 MONTH)) AS month
  FROM (SELECT min(fill_month) AS first_month, max(fill_month) AS last_month FROM silver.claims_final)
),
member_months AS (
  SELECT m.month, mb.plan_id, count(*) AS member_months
  FROM months m
  JOIN silver.members mb
    ON  mb.eligibility_start         <= last_day(m.month)
    AND mb.eligibility_end_effective >= m.month
  GROUP BY m.month, mb.plan_id
),
claim_totals AS (
  SELECT
    c.fill_month                                                    AS month,
    coalesce(mb.plan_id, 'UNKNOWN_MEMBER')                          AS plan_id,
    count(*)                                                        AS claims,
    sum(c.days_supply)                                              AS days_supply,
    sum(c.days_supply) / 30.0                                       AS adjusted_scripts,
    count_if(d.specialty_flag = 'Y')                                AS specialty_claims,
    sum(c.gross_cost)                                               AS gross_cost,
    sum(CASE WHEN d.specialty_flag = 'Y' THEN c.gross_cost ELSE 0 END) AS specialty_gross_cost,
    sum(c.plan_paid)                                                AS plan_paid
  FROM silver.claims_final c
  JOIN silver.drug_reference d ON d.ndc11 = c.ndc11
  LEFT JOIN silver.members mb  ON mb.member_id = c.member_id
  GROUP BY 1, 2
)
SELECT
  coalesce(ct.month, mm.month)                                              AS fill_month,
  coalesce(ct.plan_id, mm.plan_id)                                          AS plan_id,
  coalesce(mm.member_months, 0)                                             AS member_months,
  coalesce(ct.claims, 0)                                                    AS claims,
  coalesce(ct.days_supply, 0)                                               AS days_supply,
  coalesce(ct.specialty_claims, 0)                                          AS specialty_claims,
  coalesce(ct.gross_cost, 0)                                                AS gross_cost,
  coalesce(ct.specialty_gross_cost, 0)                                      AS specialty_gross_cost,
  coalesce(ct.plan_paid, 0)                                                 AS plan_paid,
  round(ct.claims * 12 * 1000.0 / nullif(mm.member_months, 0), 1)           AS claims_per_1000_members_annualised,
  round(ct.adjusted_scripts * 12 * 1000.0 / nullif(mm.member_months, 0), 1) AS adjusted_scripts_per_1000_annualised,
  round(ct.gross_cost / nullif(mm.member_months, 0), 2)                     AS gross_cost_pmpm,
  round(ct.days_supply / nullif(ct.claims, 0), 1)                           AS avg_days_supply,
  round(ct.specialty_gross_cost / nullif(ct.gross_cost, 0), 4)              AS specialty_spend_share
FROM claim_totals ct
FULL OUTER JOIN member_months mm
  ON mm.month = ct.month AND mm.plan_id = ct.plan_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. `gold.top_drugs` (view)
-- MAGIC A view on top of `drug_spend_monthly`. It's small and always current, so there's no need to store it. Filter on `rank_by_spend <= 10` for a top-10 list.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.top_drugs
COMMENT 'Gold: drugs ranked by gross cost and by claim count within each fill year.'
AS
SELECT
  year(fill_month)                                                          AS fill_year,
  drug_name,
  therapeutic_class,
  specialty_flag,
  brand_generic_flag,
  sum(claim_count)                                                          AS claims,
  sum(gross_cost)                                                           AS gross_cost,
  sum(plan_paid)                                                            AS plan_paid,
  rank() OVER (PARTITION BY year(fill_month) ORDER BY sum(gross_cost) DESC)  AS rank_by_spend,
  rank() OVER (PARTITION BY year(fill_month) ORDER BY sum(claim_count) DESC) AS rank_by_claims
FROM gold.drug_spend_monthly
GROUP BY year(fill_month), drug_name, therapeutic_class, specialty_flag, brand_generic_flag;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. `gold.rebate_eligible_claims`
-- MAGIC One row per **brand** claim that matches a rebate contract active on its fill date.
-- MAGIC
-- MAGIC **Matching:** same manufacturer, `fill_date` within the contract's effective dates, and either
-- MAGIC * an **NDC-level** contract for the claim's `ndc11`, or
-- MAGIC * a **class-level** contract for the claim's `therapeutic_class`.
-- MAGIC
-- MAGIC **Precedence (no double counting):** a claim can match both. Exactly one contract wins: NDC-level beats class-level,
-- MAGIC then the higher %, then the lowest `contract_id` as a deterministic tie-breaker. `estimated_rebate_if_stacked` shows what a naive join
-- MAGIC that summed every match would have reported. The difference is the double count avoided.
-- MAGIC
-- MAGIC **Assumption:** `estimated_rebate = ingredient_cost × rebate_pct`. Real contracts usually apply the % to WAC (list price) per unit.
-- MAGIC This data has no WAC price list, so ingredient cost stands in for it. That would be the first change with real data.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.rebate_eligible_claims
COMMENT 'Gold: brand claims matched to the one rebate contract that applies (NDC-level beats class-level), with estimated rebate = ingredient_cost x rebate_pct.'
AS
WITH brand_claims AS (
  SELECT
    c.claim_id, c.fill_date, c.fill_month, c.member_id, c.pharmacy_npi, c.ndc11,
    d.drug_name, d.manufacturer, d.therapeutic_class, d.specialty_flag,
    c.quantity_dispensed, c.ingredient_cost, c.gross_cost, c.plan_paid,
    c.member_found, c.is_eligible_on_fill_date
  FROM silver.claims_final c
  JOIN silver.drug_reference d ON d.ndc11 = c.ndc11
  WHERE d.brand_generic_flag = 'B'
),
matches AS (
  SELECT
    b.*,
    k.contract_id,
    k.contract_grain,
    k.rebate_pct,
    count(*)          OVER (PARTITION BY b.claim_id) AS matched_contracts,
    sum(k.rebate_pct) OVER (PARTITION BY b.claim_id) AS stacked_rebate_pct
  FROM brand_claims b
  JOIN silver.rebate_contracts k
    ON  k.manufacturer = b.manufacturer
    AND b.fill_date BETWEEN k.effective_start AND k.effective_end
    AND (   (k.contract_grain = 'NDC'   AND k.ndc11 = b.ndc11)
         OR (k.contract_grain = 'CLASS' AND k.therapeutic_class = b.therapeutic_class))
)
SELECT
  claim_id, fill_date, fill_month, member_id, pharmacy_npi, ndc11,
  drug_name, manufacturer, therapeutic_class, specialty_flag,
  quantity_dispensed, ingredient_cost, gross_cost, plan_paid,
  member_found, is_eligible_on_fill_date,
  contract_id, contract_grain, rebate_pct,
  CAST(round(ingredient_cost * rebate_pct / 100, 2) AS DECIMAL(14,2))          AS estimated_rebate,
  matched_contracts,
  CAST(round(ingredient_cost * stacked_rebate_pct / 100, 2) AS DECIMAL(14,2))  AS estimated_rebate_if_stacked
FROM matches
QUALIFY row_number() OVER (
  PARTITION BY claim_id
  ORDER BY CASE contract_grain WHEN 'NDC' THEN 1 ELSE 2 END, rebate_pct DESC, contract_id
) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Reconcile gold back to silver
-- MAGIC Every gold total must tie back to `silver.claims_final`. If any check is false, the last cell stops the job with `raise_error`.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gold_reconciliation AS
WITH s AS (SELECT count(*) AS claims, sum(gross_cost) AS gross_cost, sum(plan_paid) AS plan_paid FROM silver.claims_final)
SELECT 'drug_spend_monthly: claim_count = silver claims' AS check_name,
       (SELECT sum(claim_count) FROM gold.drug_spend_monthly) = (SELECT claims FROM s) AS ok
UNION ALL
SELECT 'drug_spend_monthly: gross_cost = silver gross_cost',
       (SELECT sum(gross_cost) FROM gold.drug_spend_monthly) = (SELECT gross_cost FROM s)
UNION ALL
SELECT 'drug_spend_monthly: plan_paid = silver plan_paid',
       (SELECT sum(plan_paid) FROM gold.drug_spend_monthly) = (SELECT plan_paid FROM s)
UNION ALL
SELECT 'utilisation: claims = silver claims',
       (SELECT sum(claims) FROM gold.utilisation) = (SELECT claims FROM s)
UNION ALL
SELECT 'utilisation: gross_cost = silver gross_cost',
       (SELECT sum(gross_cost) FROM gold.utilisation) = (SELECT gross_cost FROM s)
UNION ALL
SELECT 'rebate_eligible_claims: one row per claim',
       (SELECT count(*) = count(DISTINCT claim_id) FROM gold.rebate_eligible_claims)
UNION ALL
SELECT 'rebate_eligible_claims: every fill date inside its contract dates',
       (SELECT count_if(NOT (r.fill_date BETWEEN k.effective_start AND k.effective_end)) = 0
        FROM gold.rebate_eligible_claims r JOIN silver.rebate_contracts k USING (contract_id))
UNION ALL
SELECT 'rebate_eligible_claims: brand claims only',
       (SELECT count(*) = 0 FROM gold.rebate_eligible_claims r
        JOIN silver.drug_reference d ON d.ndc11 = r.ndc11 WHERE d.brand_generic_flag <> 'B');

SELECT check_name, CASE WHEN ok THEN 'PASS' ELSE 'FAIL' END AS result FROM gold_reconciliation;

-- COMMAND ----------

-- Stop the job if any reconciliation check failed (coalesce: a NULL result counts as a failure)
SELECT CASE WHEN count_if(NOT coalesce(ok, false)) > 0
            THEN raise_error('Gold reconciliation failed. See the gold_reconciliation results above.')
            ELSE 'Gold reconciliation: PASS' END AS gold_reconciliation
FROM gold_reconciliation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. `gold.pipeline_audit`
-- MAGIC Row counts and plan-paid dollars for each layer, recorded **per pipeline run**, from the landing files through to gold.
-- MAGIC
-- MAGIC * **Run id:** taken from the latest `silver.dq_results` run. In the Job, DQ runs just before gold with `{{job.run_id}}`, so the audit
-- MAGIC   rows carry the same id as the DQ results that cleared this run.
-- MAGIC * **`MERGE` on (run_id, table_name):** re-running gold for the same run updates its rows instead of adding duplicates, while
-- MAGIC   earlier runs stay as history.
-- MAGIC * Bronze dollars include reversals (negative) and duplicates, so they are **not** expected to equal silver. The waterfall in notebook 03
-- MAGIC   explains the gap.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold.pipeline_audit (
  run_id       STRING,
  layer        STRING,
  table_name   STRING,
  row_count    BIGINT,
  plan_paid    DECIMAL(38,2),
  note         STRING,
  audited_at   TIMESTAMP
)
COMMENT 'Gold: row counts and plan-paid totals per layer per pipeline run, for reconciliation. Upserted with MERGE on (run_id, table_name).';

-- COMMAND ----------

MERGE INTO gold.pipeline_audit AS t
USING (
  WITH run AS (SELECT max_by(run_id, run_ts) AS run_id FROM silver.dq_results),
  counts AS (
    SELECT 1 AS seq, 'landing' AS layer, 'landing/claims (manifest)' AS table_name,
           CAST(row_counts.claims_total AS BIGINT) AS row_count, CAST(NULL AS DECIMAL(22,2)) AS plan_paid,
           'Rows the generator wrote to the claim files' AS note
    FROM read_files('/Volumes/portfolio/raw/landing/_manifest/', format => 'json', multiLine => true)
    UNION ALL
    SELECT 2, 'bronze', 'bronze.claims', count(*), sum(try_cast(plan_paid AS DECIMAL(12,2))),
           'All rows as received: paid, reversed (negative $), duplicates'
    FROM bronze.claims
    UNION ALL
    SELECT 3, 'silver', 'silver.claims_final', count(*), sum(plan_paid),
           'Final paid claims: netted, deduplicated, valid NDC'
    FROM silver.claims_final
    UNION ALL
    SELECT 4, 'silver', 'silver.claims_rejected', count(*), sum(plan_paid),
           concat('By reason: ', (SELECT concat_ws(', ', collect_list(concat(reason_code, ' ', cnt)))
                                  FROM (SELECT reason_code, count(*) AS cnt FROM silver.claims_rejected GROUP BY 1 ORDER BY 1)))
    FROM silver.claims_rejected
    UNION ALL
    SELECT 5, 'silver', 'silver.dq_results (latest run)', count(*), CAST(NULL AS DECIMAL(22,2)),
           concat(count_if(status = 'PASS'), ' PASS, ', count_if(status = 'WARN'), ' WARN, ', count_if(status = 'FAIL'), ' FAIL')
    FROM silver.dq_results_latest
    UNION ALL
    SELECT 6, 'gold', 'gold.drug_spend_monthly', sum(claim_count), sum(plan_paid), 'Claims rolled up by month x drug'
    FROM gold.drug_spend_monthly
    UNION ALL
    SELECT 7, 'gold', 'gold.utilisation', sum(claims), sum(plan_paid), 'Claims rolled up by month x plan'
    FROM gold.utilisation
    UNION ALL
    SELECT 8, 'gold', 'gold.rebate_eligible_claims', count(*), sum(plan_paid),
           concat('Estimated rebate $', format_number(sum(estimated_rebate), 2))
    FROM gold.rebate_eligible_claims
  )
  SELECT run.run_id, counts.layer, counts.table_name, counts.row_count,
         CAST(counts.plan_paid AS DECIMAL(38,2)) AS plan_paid,  -- sums of sums widen the decimal type; pin it to the column's type
         counts.note,
         current_timestamp() AS audited_at
  FROM counts CROSS JOIN run
) AS s
ON t.run_id = s.run_id AND t.table_name = s.table_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT layer, table_name, row_count, plan_paid, note
FROM gold.pipeline_audit
WHERE run_id = (SELECT max_by(run_id, run_ts) FROM silver.dq_results)
ORDER BY CASE layer WHEN 'landing' THEN 1 WHEN 'bronze' THEN 2 WHEN 'silver' THEN 3 ELSE 4 END, table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Headline insights
-- MAGIC The numbers behind the portfolio write-up.

-- COMMAND ----------

-- Specialty: share of claims vs share of spend
SELECT
  specialty_flag,
  sum(claim_count)                                                      AS claims,
  round(100 * sum(claim_count) / sum(sum(claim_count)) OVER (), 1)      AS pct_of_claims,
  round(sum(gross_cost) / 1e6, 1)                                       AS gross_cost_musd,
  round(100 * sum(gross_cost) / sum(sum(gross_cost)) OVER (), 1)        AS pct_of_spend
FROM gold.drug_spend_monthly
GROUP BY specialty_flag
ORDER BY specialty_flag DESC;

-- COMMAND ----------

-- Utilisation for the whole book (all plans, all months)
SELECT
  sum(member_months)                                                                  AS member_months,
  sum(claims)                                                                         AS claims,
  round(sum(CASE WHEN plan_id <> 'UNKNOWN_MEMBER' THEN claims END) * 12000.0 / sum(member_months), 1) AS claims_per_1000_members_annualised,
  round(sum(CASE WHEN plan_id <> 'UNKNOWN_MEMBER' THEN gross_cost END) / sum(member_months), 2)       AS gross_cost_pmpm
FROM gold.utilisation;

-- COMMAND ----------

-- Rebates: what precedence prevented, and what unmapped NDCs leave on the table
SELECT
  count(*)                                                         AS rebate_eligible_claims,
  round(sum(estimated_rebate) / 1e6, 2)                            AS estimated_rebate_musd,
  count_if(matched_contracts > 1)                                  AS claims_matching_2plus_contracts,
  round(sum(estimated_rebate_if_stacked - estimated_rebate) / 1e6, 2) AS double_count_avoided_musd,
  (SELECT round(sum(plan_paid) / 1e6, 2) FROM silver.claims_rejected
   WHERE reason_code = 'NDC_NOT_IN_REFERENCE')                     AS plan_paid_on_unmapped_ndcs_musd
FROM gold.rebate_eligible_claims;

-- COMMAND ----------

-- Top 10 drugs by spend in the latest year
SELECT rank_by_spend, drug_name, therapeutic_class, specialty_flag, claims, round(gross_cost / 1e6, 2) AS gross_cost_musd
FROM gold.top_drugs
WHERE fill_year = (SELECT max(fill_year) FROM gold.top_drugs) AND rank_by_spend <= 10
ORDER BY rank_by_spend;
