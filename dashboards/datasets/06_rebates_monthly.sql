-- Dataset: rebates_monthly
-- Estimated rebate per month, by contract level and manufacturer, from gold.rebate_eligible_claims.
-- double_count_avoided = what a naive join that stacked every matching contract would have
-- over-reported. It shows why the one-contract-per-claim precedence rule matters.

SELECT
  fill_month,
  CASE contract_grain WHEN 'NDC' THEN 'NDC-level contract' ELSE 'Class-level contract' END AS contract_level,
  manufacturer,
  therapeutic_class,
  count(*)                                              AS rebate_claims,
  sum(ingredient_cost)                                  AS ingredient_cost,
  sum(estimated_rebate)                                 AS estimated_rebate,
  sum(estimated_rebate_if_stacked - estimated_rebate)   AS double_count_avoided
FROM portfolio.gold.rebate_eligible_claims
GROUP BY ALL;
