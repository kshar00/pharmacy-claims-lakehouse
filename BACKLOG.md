# Backlog

Small, real improvements, each about one sitting and one commit. Pick the next unticked item; tick it in the same commit that does it. Bigger items are at the bottom.

Run the tests before every push: `python -m pytest tests` (they also run on GitHub on every push).

## Done
- [x] Unit tests for the generator helpers (NPI check digit, delivery labels, config) and repo consistency checks (job ↔ notebooks, dashboard ↔ dataset SQL, doc links), run by GitHub Actions on every push

## Quick wins (10–20 minutes)
- [ ] `CHANGELOG.md`: what changed and when, starting from the first release
- [ ] Add a status badge for the tests workflow to the top of the README
- [ ] Dependabot config (`.github/dependabot.yml`) to keep the GitHub Actions versions current
- [ ] Add `ruff` linting for the Python notebooks to CI, and fix whatever it flags
- [ ] Weekly scheduled check of external links in the docs (for example with the lychee action)

## Tests (20–40 minutes)
- [ ] Test `new_ndc` in the generator: generated NDCs are 11 digits, unique, and each labeler code belongs to one manufacturer
- [ ] Test `negate_money`: a reversal's money columns are the exact negatives of the original
- [ ] Make the claim count a widget (`n_base_claims`, default 920,000), then add an end-to-end test that runs the generator with 5,000 claims into a temp folder and checks the manifest against the files written
- [ ] Write the NDC normalisation rule as a table of test cases (input → expected 11 digits) in `tests/`, and document it in `docs/architecture.md`

## Docs (20–40 minutes)
- [ ] `docs/data_dictionary.md`: every gold column with its grain, type and definition
- [ ] `docs/runbook.md`: what to do when a DQ check fails, a reconciliation fails, or a file arrives late
- [ ] Dashboard subtitle still says "Synthetic PBM claims": change it in Databricks to "Synthetic pharmacy claims", re-export, and replace `dashboards/pharmacy_claims_lakehouse.lvdash.json` (the consistency test will confirm the SQL still matches)

## Features (an evening each)
- [ ] `silver.dq_trend` view: pass/warn/fail counts per run, plus a line chart on dashboard page 2
- [ ] Members as SCD Type 2, so eligibility is checked against the member record that was valid on the fill date
- [ ] One gold mart rebuilt in dbt (on Databricks), compared with the SQL notebook version
- [ ] Deploy notebooks and the job with a Databricks Asset Bundle (`databricks.yml`)

## Regular maintenance
- **Monthly:** run the job in Databricks and confirm 33 PASS / 1 WARN and the reconciliation PASS. Note the date in the CHANGELOG.
- **When Databricks changes the dashboard format:** re-export the dashboard and run the tests.
