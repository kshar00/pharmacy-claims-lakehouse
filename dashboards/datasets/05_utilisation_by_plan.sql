-- Dataset: utilisation_by_plan
-- Whole-period utilisation per plan, for comparing plans side by side.
-- Same rule as utilisation_monthly: ratio of sums, never an average of monthly rates.

SELECT
  plan_id,
  sum(member_months)                                                  AS member_months,
  sum(claims)                                                         AS total_claims,
  round(sum(claims) * 12000.0 / sum(member_months), 0)                AS claims_per_1000_per_year,
  round(sum(gross_cost) / sum(member_months), 2)                      AS gross_cost_pmpm,
  round(100 * sum(specialty_gross_cost) / sum(gross_cost), 1)         AS specialty_pct_of_spend
FROM portfolio.gold.utilisation
WHERE plan_id <> 'UNKNOWN_MEMBER'
GROUP BY plan_id
ORDER BY plan_id;
