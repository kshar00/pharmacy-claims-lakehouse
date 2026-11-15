-- Dataset: rejected_claims
-- Claim rows silver could not use, by reason. NDC_NOT_IN_REFERENCE is the "rebate leakage" story:
-- claims for real-looking NDCs that are missing from the drug file can't be matched to a rebate
-- contract, so any rebate on them is never invoiced.
-- Reads a silver table on purpose: claims_rejected is already a small, curated exceptions table.

SELECT
  reason_code,
  CASE reason_code
    WHEN 'INVALID_NDC_FORMAT'     THEN 'NDC cannot be parsed to 11 digits'
    WHEN 'NDC_NOT_IN_REFERENCE'   THEN 'Valid NDC, missing from drug file'
    WHEN 'ORPHAN_REVERSAL'        THEN 'Reversal of a claim never received'
    WHEN 'MISSING_REQUIRED_FIELD' THEN 'Required field is blank'
    ELSE reason_code
  END                                     AS reason,
  count(*)                                AS rejected_claims,
  round(sum(plan_paid), 2)                AS plan_paid
FROM portfolio.silver.claims_rejected
GROUP BY reason_code
ORDER BY rejected_claims DESC;
