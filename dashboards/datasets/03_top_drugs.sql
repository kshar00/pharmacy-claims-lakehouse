-- Dataset: top_drugs
-- Top 10 drugs by gross cost in the latest fill year, from the gold.top_drugs view.
-- cost_per_claim makes the specialty story obvious: a few hundred claims of a $18k drug outspend
-- ten thousand claims of a $600 drug.

SELECT
  rank_by_spend                                                  AS spend_rank,
  drug_name,
  therapeutic_class,
  CASE specialty_flag     WHEN 'Y' THEN 'Specialty' ELSE 'Traditional' END AS drug_type,
  CASE brand_generic_flag WHEN 'B' THEN 'Brand'     ELSE 'Generic'     END AS brand_generic,
  claims,
  round(gross_cost / 1e6, 2)                                     AS gross_cost_musd,
  round(gross_cost / claims, 0)                                  AS cost_per_claim,
  fill_year
FROM portfolio.gold.top_drugs
WHERE fill_year = (SELECT max(fill_year) FROM portfolio.gold.top_drugs)
  AND rank_by_spend <= 10
ORDER BY spend_rank;
