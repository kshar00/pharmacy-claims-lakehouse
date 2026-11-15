-- Dataset: spend_monthly
-- gold.drug_spend_monthly with readable labels. Feeds the spend trend and therapeutic class charts,
-- and the page filters (date range, therapeutic class, drug type).
--
-- Only additive measures (counts and dollars) are exposed here, so any widget can safely SUM them
-- across months, drugs or classes. About 11.5k rows: small enough for the 2X-Small warehouse.

SELECT
  fill_month,
  drug_name,
  therapeutic_class,
  CASE specialty_flag     WHEN 'Y' THEN 'Specialty' ELSE 'Traditional' END AS drug_type,
  CASE brand_generic_flag WHEN 'B' THEN 'Brand'     ELSE 'Generic'     END AS brand_generic,
  manufacturer,
  claim_count,
  total_days_supply,
  gross_cost,
  member_copay,
  plan_paid,
  claims_outside_eligibility,
  plan_paid_outside_eligibility
FROM portfolio.gold.drug_spend_monthly;
