-- Dataset: kpi_headline
-- One row of whole-book headline numbers. Feeds the counter tiles on both dashboard pages.
--
-- Why one pre-computed row: ratios (specialty share, PMPM, claims per 1,000) must be "sum of the
-- numerator ÷ sum of the denominator". Averaging monthly or per-plan ratios in a widget would give the
-- wrong answer, so the ratios are computed here in SQL, once, and the tiles just display them.
-- Money is shown in $M (rounded) so the tiles need no number formatting.

WITH spend AS (
  SELECT
    sum(claim_count)                                                AS claims,
    sum(gross_cost)                                                 AS gross_cost,
    sum(plan_paid)                                                  AS plan_paid,
    sum(CASE WHEN specialty_flag = 'Y' THEN gross_cost  ELSE 0 END) AS specialty_gross_cost,
    sum(CASE WHEN specialty_flag = 'Y' THEN claim_count ELSE 0 END) AS specialty_claims
  FROM portfolio.gold.drug_spend_monthly
),
util AS (  -- rates use only members we can place in a plan (UNKNOWN_MEMBER has no denominator)
  SELECT
    sum(member_months)                                                      AS member_months,
    sum(CASE WHEN plan_id <> 'UNKNOWN_MEMBER' THEN claims     END)          AS known_claims,
    sum(CASE WHEN plan_id <> 'UNKNOWN_MEMBER' THEN gross_cost END)          AS known_gross_cost
  FROM portfolio.gold.utilisation
),
rebate AS (
  SELECT
    count(*)                                                AS rebate_claims,
    sum(estimated_rebate)                                   AS estimated_rebate,
    sum(estimated_rebate_if_stacked - estimated_rebate)     AS double_count_avoided
  FROM portfolio.gold.rebate_eligible_claims
),
rejects AS (
  SELECT
    count(*)                                                                    AS rejected_claims,
    sum(CASE WHEN reason_code = 'NDC_NOT_IN_REFERENCE' THEN plan_paid END)      AS unmapped_ndc_plan_paid
  FROM portfolio.silver.claims_rejected
)
SELECT
  s.claims                                                        AS total_claims,
  round(s.gross_cost / 1e6, 1)                                    AS gross_cost_musd,
  round(s.plan_paid  / 1e6, 1)                                    AS plan_paid_musd,
  round(100 * s.specialty_gross_cost / s.gross_cost, 1)           AS specialty_pct_of_spend,
  round(100 * s.specialty_claims / s.claims, 1)                   AS specialty_pct_of_claims,
  u.member_months,
  round(u.known_claims * 12000.0 / u.member_months, 0)            AS claims_per_1000_per_year,
  round(u.known_gross_cost / u.member_months, 2)                  AS gross_cost_pmpm,
  r.rebate_claims,
  round(r.estimated_rebate / 1e6, 2)                              AS estimated_rebate_musd,
  round(r.double_count_avoided / 1e6, 2)                          AS double_count_avoided_musd,
  j.rejected_claims,
  round(j.unmapped_ndc_plan_paid / 1e6, 2)                        AS unmapped_ndc_plan_paid_musd
FROM spend s
CROSS JOIN util u
CROSS JOIN rebate r
CROSS JOIN rejects j;
