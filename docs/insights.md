# Insights from the gold layer

> **The data is synthetic.** It was generated to behave like a PBM claims book (specialty cost concentration, reversals, duplicates, messy NDCs, eligibility gaps), so the numbers show **the method**, not real-world facts. Every figure below comes from the gold layer and can be reproduced with the SQL in the appendix.

**Book:** 887,876 final paid claims, Jan 2024 – Dec 2025 · $233.4M gross cost · $214.0M plan paid · 823,543 eligible member-months · $282.90 gross cost PMPM · 12,912 claims per 1,000 members per year.

---

## 1. Specialty is 1.8% of claims and 57.1% of spend

| | Claims | % of claims | Gross cost | % of spend |
|---|---:|---:|---:|---:|
| Specialty | 15,820 | 1.8% | $133.3M | **57.1%** |
| Traditional | 872,056 | 98.2% | $100.1M | 42.9% |

The top drug by spend, **Praxilia 200 MG TAB** (oncology, specialty), cost $9.49M in 2025 from **530 claims**, about **$17,900 per claim**. The #3 drug, **Exadra 10 MG TAB** (antidiabetic, traditional), needed **10,627 claims** at about **$627 per claim** to reach $6.66M.

**So what:** cost management belongs on a small number of specialty members and drugs: prior authorisation, specialty pharmacy channel management, biosimilar and generic conversion. Generic dispensing-rate programmes, however good, act on the 43%.

## 2. Spend growth is volume, not price

| | 2024 | 2025 | Change |
|---|---:|---:|---:|
| Gross cost | $102.0M | $131.4M | +28.8% |
| Claims | 387,528 | 500,348 | +29.1% |
| Cost per claim | $263 | $263 | flat |

**So what:** with flat unit cost, the question is whether the extra claims come from more members or from more use per member. The dashboard's PMPM and claims-per-1,000 trends answer it, because they divide by eligible member-months rather than headcount.

## 3. Rebates: $17.14M estimated, and a $0.58M double count avoided

- **90,659** brand claims match an active rebate contract, for an estimated **$17.14M** (ingredient cost × contract %).
- NDC-level contracts earn 96% of it ($16.52M); class-level contracts add $0.63M.
- **20,029** claims match **two or more** contracts (an NDC-level and a class-level contract for the same manufacturer). Summing every match would overstate the rebate by **$0.58M (3.4%)**. Gold applies one winning contract per claim: NDC-level beats class-level, then the higher %.

**So what:** a rebate accrual built on a simple join overstates what the manufacturer will pay, and the gap surfaces later as a disputed invoice. Precedence rules belong in the data model, not in a spreadsheet after the fact.

## 4. Rebate leakage from NDCs that can't be mapped

| Rejected reason | Claims | Plan paid |
|---|---:|---:|
| `NDC_NOT_IN_REFERENCE` (valid 11-digit NDC, missing from the drug file) | 6,409 | $1.38M |
| `INVALID_NDC_FORMAT` (can't be parsed to 11 digits) | 7,363 | $1.85M |

These claims were paid, but without a drug record they can't be classed, reported as spend by drug or **tested for rebate eligibility**. Across the book, estimated rebate is about 7.3% of gross cost. At that rate, the unmapped claims carry roughly **$0.10M** of rebate that is never invoiced, rising to about **$0.24M** if the invalid-format claims are fixed at source too.

**So what:** the cheapest rebate dollar is the one already earned. Keeping the drug reference file current (new NDCs, repackagers) is a revenue control, not just a data-hygiene task.

## 5. 1.5% of claims were paid outside member eligibility

**13,292** final claims (1.50%) were filled on a date outside the member's eligibility window, about **$3.1M** of plan paid. They are **flagged, not removed**: they were paid, so removing them would hide the money.

**So what:** this is a recovery work-list, by plan and pharmacy, for the eligibility and claims-recovery teams. It is also the one data quality check over tolerance (WARN at 1.50% against 1.0%), which is the pipeline doing its job: the business gets its numbers, and the data owner gets an alert.

## 6. Without NDC normalisation, 39% of claims would be unattributed

Only 61% of final claims arrived with a clean 11-digit NDC. The rest came as 10-digit dashed (225,089), 11-digit dashed (45,311), or with leading zeros lost (76,623): **347,023 claims, 39%**. Silver normalises each to the 5-4-2 11-digit form before joining to the drug file.

**So what:** a pipeline that joins on NDC as received would silently drop well over a third of claims from every drug-level report, and nothing would error. That's why silver normalises first and reconciles every row.

---

## A note on what this data can't show

The six plans in this book have similar PMPM ($278–$292) and specialty share (57–58%). The generator didn't give plans different benefit designs, so plan comparisons here aren't meaningful. With real data, this is where the analysis would go next: by plan, line of business and channel (retail, mail, specialty).

---

## Appendix: reproduce the numbers

Run in the SQL editor on the Serverless Starter Warehouse.

```sql
-- 1. Specialty share
SELECT specialty_flag, sum(claim_count) AS claims, round(sum(gross_cost)/1e6, 1) AS gross_cost_musd,
       round(100 * sum(gross_cost) / sum(sum(gross_cost)) OVER (), 1) AS pct_of_spend
FROM portfolio.gold.drug_spend_monthly GROUP BY specialty_flag;

-- 2. Growth by year
SELECT year(fill_month) AS yr, sum(claim_count) AS claims, round(sum(gross_cost)/1e6, 1) AS gross_cost_musd,
       round(sum(gross_cost) / sum(claim_count), 0) AS cost_per_claim
FROM portfolio.gold.drug_spend_monthly GROUP BY 1 ORDER BY 1;

-- 3. Rebates and double count avoided
SELECT contract_grain, count(*) AS claims, round(sum(estimated_rebate)/1e6, 2) AS rebate_musd,
       count_if(matched_contracts > 1) AS multi_match,
       round(sum(estimated_rebate_if_stacked - estimated_rebate)/1e6, 2) AS double_count_avoided_musd
FROM portfolio.gold.rebate_eligible_claims GROUP BY ROLLUP(contract_grain);

-- 4. Rejected claims by reason
SELECT reason_code, count(*) AS claims, round(sum(plan_paid)/1e6, 2) AS plan_paid_musd
FROM portfolio.silver.claims_rejected GROUP BY reason_code;

-- 5. Paid outside eligibility
SELECT sum(claims_outside_eligibility) AS claims, round(sum(plan_paid_outside_eligibility)/1e6, 2) AS plan_paid_musd
FROM portfolio.gold.drug_spend_monthly;

-- 6. NDC format as received, for final claims
SELECT ndc_source_format, count(*) AS claims, round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM portfolio.silver.claims_final GROUP BY ndc_source_format ORDER BY claims DESC;
```
