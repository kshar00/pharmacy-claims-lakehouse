# CLAUDE.md – Pharmacy Claims Lakehouse on Databricks Free Edition

## Project purpose

A portfolio project for Kunal, an analytics manager with 11+ years in PBM and specialty pharmacy analytics. It rebuilds a legacy SQL Server / Alteryx / Azure claims pipeline on Databricks to show modern lakehouse design. The audience is recruiters and hiring managers. Clarity of design matters more than volume of code.

## Hard constraints

- Must run entirely on **Databricks Free Edition**. Do not use any paid-tier features.
- Serverless compute only. No custom clusters or cluster configs.
- One 2X-Small SQL warehouse; at most 5 concurrent job tasks; daily compute quota.
- Python and SQL only. No Scala or R.
- Unity Catalog with a single metastore. Use one catalog, e.g. `portfolio`.
- Synthetic data only. No real PHI or employer data. Keep total volume modest (target 1–3 million claim rows).
- Non-commercial personal project.

## Architecture (medallion)

1. **Ingest** – Python generator writes synthetic CSV/Parquet files to a Unity Catalog volume (`/Volumes/portfolio/raw/landing`).
2. **Bronze** – Raw files loaded as-is into Delta tables with `_ingested_at` and `_source_file` columns.
3. **Silver** – Cleansed and conformed data: NDC standardised to 11-digit 5-4-2 format, claim reversals netted, duplicates removed, data quality checks applied.
4. **Gold** – Business marts: drug spend, utilisation, rebate-eligible claims.
5. **Serve** – Databricks SQL dashboard on the 2X-Small warehouse.
6. **Orchestrate** – A single Databricks Job chaining the notebooks in order.

## Schemas

- `portfolio.bronze`
- `portfolio.silver`
- `portfolio.gold`
- `portfolio.raw` (volume for landing files)

## Synthetic source data

- **claims** – claim\_id, rx\_number, fill\_date, member\_id, pharmacy\_npi, prescriber\_npi, ndc (mixed formats on purpose: 10-digit with dashes, 11-digit, missing leading zeros), quantity\_dispensed, days\_supply, ingredient\_cost, dispensing\_fee, copay, plan\_paid, claim\_status (paid / reversed), reversal\_of\_claim\_id.
- **drug\_reference** – ndc11, drug\_name, brand\_generic\_flag, specialty\_flag, therapeutic\_class, manufacturer, package\_size, unit\_of\_measure.
- **members** – member\_id, plan\_id, birth\_year, gender, state, eligibility\_start, eligibility\_end.
- **pharmacies** – pharmacy\_npi, name, type (retail / specialty / mail), state.
- **rebate\_contracts** – manufacturer, ndc11 or therapeutic\_class, rebate\_pct, effective\_start, effective\_end.

Deliberately inject realistic messiness: duplicate claims, reversals, invalid NDCs, members outside eligibility, late-arriving files.

## Silver rules

- Normalise every NDC to ndc11; route unmappable NDCs to a `silver.claims_rejected` table with a reason code.
- Net reversals so only final paid claims remain in `silver.claims_final`.
- Deduplicate on (rx\_number, fill\_date, pharmacy\_npi) keeping the latest ingestion.
- Flag claims outside member eligibility.
- Data quality checks in plain PySpark/SQL (row counts, null checks, referential checks). Write results to `silver.dq_results`.

## Gold marts

- `gold.drug_spend_monthly` – spend by month, drug, therapeutic class, specialty flag.
- `gold.utilisation` – claims per 1,000 members, days supply, top drugs.
- `gold.rebate_eligible_claims` – claims matched to active rebate contracts with estimated rebate amount.
- `gold.pipeline_audit` – row counts per layer per run for reconciliation.

## Repo layout

```
/notebooks
  00_setup_catalog.sql
  01_generate_synthetic_data.py
  02_bronze_ingest.py
  03_silver_claims.py
  04_silver_dq.py
  05_gold_marts.sql
/jobs
  pipeline_job.json
/dashboards
  README.md
/docs
  architecture.md
  design_decisions.md
README.md
CLAUDE.md
```

## Working agreements for Claude Code

- Build one layer at a time; stop after each step so Kunal can review and run it.
- Explain design choices in comments and in `docs/design_decisions.md`. Kunal must be able to explain every decision in an interview.
- Prefer readable, idempotent code (CREATE OR REPLACE, MERGE) over clever code.
- Never suggest paid features (no classic clusters, no Scala, no account console).
- Keep data volumes small enough to stay within the daily quota.

## Portfolio outputs

- Public GitHub repo with README, architecture diagram and design decisions.
- Screenshots and a 2-minute walkthrough video of the job run and dashboard.
- A short insight write-up from the gold layer (e.g. specialty share of spend, rebate leakage from unmapped NDCs).
