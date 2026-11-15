-- Dataset: utilisation_monthly
-- Whole-book utilisation per month: claims per 1,000 members (annualised) and gross cost PMPM.
--
-- gold.utilisation is month x plan. Rates are re-computed here as sum(numerator) / sum(member_months)
-- for each month, never averaged. Claims for members missing from the member file (UNKNOWN_MEMBER)
-- have no member-months, so they are left out of the rates (but still counted in total_claims).

SELECT
  fill_month,
  sum(member_months)                                                                         AS member_months,
  sum(claims)                                                                                AS total_claims,
  round(sum(CASE WHEN plan_id <> 'UNKNOWN_MEMBER' THEN claims END) * 12000.0
        / sum(member_months), 0)                                                             AS claims_per_1000_per_year,
  round(sum(CASE WHEN plan_id <> 'UNKNOWN_MEMBER' THEN gross_cost END) / sum(member_months), 2) AS gross_cost_pmpm,
  round(sum(days_supply) / sum(claims), 1)                                                   AS avg_days_supply
FROM portfolio.gold.utilisation
GROUP BY fill_month
ORDER BY fill_month;
