# Design decisions

Each decision gives the choice, the reason, and the alternative considered. They are short on purpose so each can be explained in an interview in about 30 seconds.

---

## Step 1 – Catalog setup and synthetic data

### D1. One catalog, four schemas, one managed volume
**Choice:** `portfolio` catalog with `raw`, `bronze`, `silver`, `gold` schemas. Landing files go in the managed volume `portfolio.raw.landing`.
**Why:** Free Edition has a single Unity Catalog metastore, and one catalog keeps permissions and lineage in one place. A separate `raw` schema that holds *only* the volume keeps files and tables apart, so `bronze` contains nothing but Delta tables.
**Why managed, not external:** Free Edition gives Databricks-managed default storage and no way to register cloud credentials. Managed objects also mean `DROP` cleans up the storage too.
**Alternative:** one schema per source system (common in larger estates). That's overkill for five sources.

### D2. `IF NOT EXISTS` for setup DDL, not `CREATE OR REPLACE`
**Why:** setup must be safe to re-run from the Job at any time. Spark SQL has no `CREATE OR REPLACE SCHEMA`, and replacing a schema would mean dropping its tables. `CREATE OR REPLACE` is kept for *tables* in later layers, where rebuilding is the point. A full reset (`DROP CATALOG ... CASCADE`) is at the bottom of notebook 00, commented out, so it cannot run by accident.

### D3. Generate with numpy + pandas on the driver, not Spark
**Why:** about 1M rows fits in roughly 200 MB and generates in under 30 seconds. Vectorised numpy expresses row-to-row rules ("this reversal points at that claim", "this rebill shares the original's key") much more readably than Spark. One seeded RNG (`random_seed` widget, default 42) gives **byte-identical files on every run**, so results are reproducible and the answer key never drifts.
**When I'd switch:** at around 100M+ rows, or if generation had to run in parallel. I'd use `spark.range()` + `rand()`.

### D4. CSV landing files, one sub-folder per entity
**Why CSV:** real PBM claim extracts and enrollment files arrive as flat files. CSV has no schema, so bronze has to deal with the real problem: if you let Spark infer types, `ndc` becomes a number and drops its leading zeros (pandas turns `ndc11` into `1.6e+10`). Bronze will read everything as strings.
**Why sub-folders:** each bronze load (Auto Loader) points at one folder, such as `landing/claims/`, with no filename filtering.

### D5. Claims split into monthly *delivery* files plus a late file
**Choice:** `claims_delivery_YYYY_MM.csv` for each month of 2024–2025, plus `claims_delivery_2026_01.csv` as the late file.
**Why:** the file a row lands in is its **arrival month**, not its fill month. A reversal filled in March and reversed in April sits in the April file. That makes "latest ingestion wins" and late-arriving data real problems instead of theory. The `write_mode` widget (`initial_only`, then `late_only`) lets the demo load the regular files first and drop the late file in afterwards, showing incremental ingestion.

### D6. Realistic NDC construction
**Choice:** each fictional manufacturer has a labeler code. NDCs are built as real 10-digit codes in 4-4-2, 5-3-2 or 5-4-1 segments, then padded to 11-digit 5-4-2.
**Why:** this is the actual NDC rule, and it is why normalisation is not just "left-pad to 11". You can only convert a 10-digit NDC correctly if you use the **dash positions** to know which segment to pad. A 4-4-2 code gets a zero at the front, a 5-3-2 code in the middle, a 5-4-1 code before the last digit.

**NDC format mix on original claims:**

| Format | Example | Share |
|---|---|---|
| 11-digit plain | `07147917803` | ~60% |
| 10-digit dashed | `7702-2948-02`, `93027-754-01` | 25% |
| 11-digit dashed | `07147-9178-03` | 5% |
| Leading zeros lost | `7147383302` | 8.5% |
| Invalid format | null, `UNKNOWN`, `ABC12-3456-78` | 0.8% |
| Well-formed but not in reference | 11 digits, unknown labeler | 0.7% |

The last two become `silver.claims_rejected` with separate reason codes, because they are different business problems. Invalid format is a data-entry or feed problem. Not-in-reference is usually a reference-data lag (for example a new launch). The second is the "rebate leakage from unmapped NDCs" story.

### D7. Fictional drug and manufacturer names, realistic classes and costs
**Why:** synthetic NDCs attached to real product names could read as real pricing. Therapeutic classes, brand/generic split and cost levels are realistic, so the gold insights hold up: **specialty is ~1.8% of claims and ~57% of spend**, close to what real commercial books show.

### D8. Reversal convention: new claim row, negated amounts, pointer to original
**Choice:** a reversal is its own row with a new `claim_id`, `claim_status = 'reversed'`, the same `rx_number` / `fill_date` / `pharmacy_npi` as the original, **negative** money fields, and `reversal_of_claim_id` pointing at the original. Quantity and days supply stay positive.
**Why:** this matches how most adjudication feeds send reversals (B2 transactions). Negative amounts mean a plain `SUM` over paid + reversed rows nets to zero. That's a useful reconciliation check even though silver nets explicitly.
**Rebills:** about half of reversed claims are re-submitted as a corrected paid claim with a new `claim_id` on the **same business key**.

### D9. Silver must net reversals *before* deduplicating (decision for Step 3, forced by Step 1 data)
**Why:** an original, its reversal and its rebill all share `(rx_number, fill_date, pharmacy_npi)`. Dedupe on that key first, and the three rows collapse into one arbitrary row. The correct order is:
1. Remove paid claims whose `claim_id` appears in any `reversal_of_claim_id` (anti-join). This works whatever the arrival order. About 1,100 reversals arrive in an earlier file than their original.
2. Dedupe the remaining paid claims on the business key, keeping the latest ingestion. This removes exact double-writes and late-file resends, and keeps the corrected copay where one was sent.

Verified locally: doing this on the generated files gives exactly the manifest's `final_paid_claims_before_ndc_rejects`.
**Generator guard:** resends are drawn only from claims that were *not* reversed. Otherwise a resend with a new `claim_id` would survive the anti-join and bring a cancelled claim back as paid. In real life that's a genuine edge case, worth mentioning but not worth building into the synthetic data.

### D10. Members: month-aligned eligibility, null end date = active
**Why:** enrollment feeds start on the 1st and term at month-end. About 75% of members have a blank `eligibility_end`, so silver must read null as "open-ended" (`COALESCE(eligibility_end, '9999-12-31')`). About 45% enrol during the claim period, which gives the 1.5% outside-eligibility claims real coverage gaps to fall into: before a mid-period start, or after a mid-period term.

### D11. Injected problems use disjoint rows, and every count goes to a manifest
**Choice:** row-level problems (outside eligibility, unknown member, unknown pharmacy, null prescriber) take disjoint slices of one random permutation, so each affected claim has exactly one problem. Actual counts go to `landing/_manifest/generation_manifest.json`.
**Why:** this gives the pipeline an **answer key**. DQ results and `gold.pipeline_audit` can be reconciled against known numbers ("1,840 unknown members injected, 1,840 flagged"), which is how you would prove a migrated pipeline matches the legacy one.

### D12. NPIs pass the real CMS Luhn check
**Why:** it costs about 10 lines and lets silver include a realistic DQ rule (valid NPI checksum). The injected "unknown pharmacy" NPIs are random 10-digit strings, so most of them also fail the checksum, giving two independent ways to catch them.

### D13. Rebate contracts at two grains, with deliberate overlap
**Choice:** brand manufacturers only. Some contracts are NDC-level (`ndc11` set, higher %, annual, about 20% not renewed for 2025). Others are class-level (`therapeutic_class` set, lower %, starting on a quarter boundary). `rebate_pct` is a percentage (22.5 means 22.5%).
**Why:** one claim can match both grains, so gold needs an explicit precedence rule (**NDC-level beats class-level**) to avoid paying the rebate twice. Lapsed contracts test the effective-date join. `contract_id` was added beyond the CLAUDE.md spec so gold can trace each estimated rebate back to its contract.

### D14. Volume sized for the daily quota
About 998k claim rows (~110 MB of CSV), 50k members, 2.5k pharmacies, ~920 NDCs, ~75 contracts. That's enough for per-1,000-member metrics and monthly trends to look real, and small enough that the whole pipeline runs several times a day on serverless without hitting the Free Edition quota.

### Not modelled (on purpose, to keep the generator readable)
- Member and pharmacy state are independent (no travel or network logic).
- One price per NDC (no AWP/WAC price history or MAC lists).
- Dates are always ISO `YYYY-MM-DD`. Mixed date formats would be a realistic next layer of messiness.
- Refills are not linked across fills of the same `rx_number`.

---

## Step 2 – Bronze ingest

### D15. Auto Loader with `trigger(availableNow=True)` for every source
**Choice:** one generic `load_to_bronze()` function. Each of the five landing folders is read by Auto Loader (`cloudFiles`) and written to `bronze.<entity>`.
**Why:** Auto Loader keeps a checkpoint of which files it has already loaded, so each run picks up only new files. That is how real monthly deliveries and late files behave. `availableNow` processes the backlog and then stops. It gives incremental loading with batch cost, it is the trigger serverless supports, and there is no always-on stream using the daily quota.
**Alternative:** `COPY INTO` is also idempotent and incremental and would work well here. I chose Auto Loader because it adds schema tracking and a rescued-data column, and it is the pattern most Databricks teams standardise on. `spark.read` + overwrite was rejected because it re-reads every file on every run and loses the notion of "what arrived when".

### D16. Everything lands as a string; no type inference
**Choice:** `cloudFiles.inferColumnTypes = false`, so every source column is `STRING` in bronze.
**Why:** inference would turn `ndc` into a number (dropping leading zeros) and could fail or null out junk values like `UNKNOWN`. Bronze's job is to keep exactly what was received. Casting is a business decision that belongs in silver, where failures can go to `claims_rejected` with a reason instead of disappearing.
**Known limitation:** CSV can't distinguish an empty string from a missing value, so both arrive as `NULL`.

### D17. Schema evolution mode `rescue`, plus an expected-columns contract
**Choice:** the schema is fixed from the first files. Any unexpected column goes to `_rescued_data`. After each load the notebook checks that every *expected* column exists and fails if one is missing.
**Why:** this treats source changes asymmetrically, on purpose. A **new** column is information to capture, not a reason to stop the pipeline. A **missing** column would silently produce an empty field downstream, so it must stop the run. `addNewColumns` was rejected because it fails the stream on every new column, and that's noisy for a scheduled job.

### D18. Lineage columns: `_ingested_at`, `_source_file`, `_source_file_modified_at`, `_run_id`
**Why four, when CLAUDE.md asks for two:** silver dedupes "keeping the latest ingestion". `_ingested_at` is one timestamp per micro-batch, so when the regular files and the late file load **in the same run** (the normal `write_mode = all` case), they share an `_ingested_at`. Silver therefore orders by `_ingested_at`, then `_source_file_modified_at` (the late file is written last), then `_source_file`. `_run_id` (the Job's run id) lets `gold.pipeline_audit` count rows per layer per run.

### D19. Checkpoints in their own volume (`raw.checkpoints`)
**Why:** checkpoints are pipeline state, not data. Keeping them out of `landing` means re-generating source files can't corrupt load state, and wiping state can't touch source files. It also keeps `bronze` to Delta tables only (D1).

### D20. Bronze is append-only and replayable
**Choice:** `delta.appendOnly = true` on every bronze table, with no casting, filtering or dedupe.
**Why:** bronze is the audit record of what the source sent. Making it immutable means any silver bug can be fixed by re-running silver from bronze, without re-requesting files. Idempotency comes from the Auto Loader checkpoint rather than `MERGE`: a second run with no new files loads 0 rows. Re-running the generator rewrites files under the same names, and Auto Loader ignores already-processed paths (`cloudFiles.allowOverwrites` defaults to false), so bronze doesn't double up. The `reset = true` widget drops the tables and checkpoints for a deliberate full reload.

### D21. Reference files go through the same append-only path
**Choice:** members, drugs, pharmacies and contracts are full snapshots, loaded exactly like claims.
**Why:** one load pattern for everything is easier to explain and operate. If a new snapshot file arrives, bronze keeps both, and silver picks the latest snapshot by `_source_file_modified_at`. That gives free history of reference data (for example, when a drug was added to the reference: the "new launch" unmapped-NDC story).

### D22. Reconciliation against the generator's manifest
**Choice:** after loading, bronze row counts per claim file are compared to the manifest. Status is `MATCH`, `NOT_LOADED`, `MISMATCH` or `UNEXPECTED_FILE`.
**Why:** this is the first link in the audit chain (landing → bronze → silver → gold). In a real migration it's the check that proves nothing was dropped or loaded twice between the legacy extract and the new platform.

---

## Step 3 – Silver claims

### D23. Order of operations: conform → net → dedupe → validate NDC → flag
**Why this order:**
- **Net before dedupe.** Original, reversal and rebill share a business key, so a key-based dedupe first would collapse them into one arbitrary row (D9).
- **Net before NDC validation.** A reversal of a bad-NDC claim must still find and cancel its original. Otherwise the reversal looks orphaned and the cancelled claim shows up as a false reject.
- **Dedupe before NDC validation.** Each rejected claim then appears once in `claims_rejected`, not once per duplicate copy, so the dollar figures behind rejects aren't inflated.

**Verified:** an independent pandas implementation of the same rules on the same files gives 901,648 claims before the NDC check (= manifest) and 887,876 in `claims_final`.

### D24. NDC normalisation: pad each dashed segment to 5-4-2
**Choice:** for any dashed NDC, split on `-` and left-pad the segments to widths 5, 4 and 2. Undashed 11 digits pass through. Undashed 8–10 digits are left-padded to 11. Anything else is `INVALID`.
**Why:** one rule handles 4-4-2, 5-3-2 and 5-4-1 correctly, because the dashes say which segment is short. That's easier to read and explain than three separate `CASE` branches. The detected `ndc_source_format` is kept on every claim, which shows which feeds send which formats.

### D25. The "zeros lost" assumption is checked by the reference lookup, not trusted
**Choice:** 8–10 undashed digits are assumed to be an 11-digit NDC with leading zeros stripped (the Excel / integer-cast failure), and padded.
**Why it's safe:** an undashed 10-digit code is genuinely ambiguous. Padding is a guess, but every normalised NDC must then exist in `drug_reference`. A wrong guess becomes `NDC_NOT_IN_REFERENCE` instead of a silent mis-mapping to the wrong drug. Fewer than 8 digits (e.g. `12345`) can't be an NDC and is `INVALID_NDC_FORMAT`.

### D26. Reject vs flag
**Reject** (to `claims_rejected`) only when the claim can't be used: a required field is missing, the NDC is unparseable or unmapped, or a reversal has nothing to reverse.
**Flag** (columns on `claims_final`) when the claim is usable but suspect: `member_found`, `is_eligible_on_fill_date`, `pharmacy_found`, `prescriber_missing`.
**Why:** an out-of-eligibility claim was still **paid**. Finance needs it in spend, and recovery teams need to find it. Dropping it would understate cost and hide the problem. Flags let gold report spend both ways.

### D27. Full rebuild with `CREATE OR REPLACE`, not incremental `MERGE`
**Why:** a reversal or a resend can arrive months after the claim it affects, so an incremental `MERGE` would need a look-back window and careful handling of late reversals. At about 1M rows, a full rebuild from bronze is quick and always correct, and re-running it is safe. Bronze being immutable (D20) is what makes this cheap and safe.
**When I'd change it:** at 100M+ rows, I'd rebuild only fill months touched by newly ingested files (claims + their reversals), using `MERGE` or `REPLACE WHERE fill_month IN (...)`.

### D28. Intermediate steps are temporary views, not tables
**Choice:** `claims_conformed`, `reversals`, `paid_after_netting`, `paid_deduped` and `orphan_reversals` are session temp views. Only reference tables, `claims_final` and `claims_rejected` are persisted.
**Why:** each rule stays a small, readable SQL step with no clutter in the catalog. The trade-off is that the views are recomputed when queried, because serverless doesn't support caching. At this volume that costs seconds.

### D29. `claims_rejected` design: one row per rejected bronze row, with money attached
**Choice:** reason codes are `MISSING_REQUIRED_FIELD`, `ORPHAN_REVERSAL`, `INVALID_NDC_FORMAT` and `NDC_NOT_IN_REFERENCE`. Each row has a readable `reason_detail` and `plan_paid`. Rows removed by netting or dedupe are **not** rejects, because they were handled correctly.
**Why:** an operations team can work the queue by reason, and gold can size it in dollars. In this data about $1.4M of plan-paid sits on unmapped NDCs. Those claims can't be matched to rebate contracts, which is the "rebate leakage" insight.

### D30. Reference tables use the latest snapshot file
**Choice:** `silver.members`, `pharmacies`, `drug_reference` and `rebate_contracts` are read from the most recent bronze file (`max_by(_source_file, (_source_file_modified_at, _ingested_at))`), typed with `try_cast`. Blank `eligibility_end` becomes `eligibility_end_effective = 9999-12-31`.
**Why:** the sources send full snapshots, so the latest file is the truth. Older snapshots stay in bronze as history. `try_cast` turns a bad value into a NULL that the DQ checks in Step 4 will count, instead of failing the whole job.

---

## Step 4 – Data quality checks

### D31. Checks as data: one list, one runner
**Choice:** each check is one `check(...)` call: an id, a category, a table, an **expectation** (SQL that should be true for every row) or a custom `failed, total` query, a severity and a tolerance. One loop runs them all.
**Why:** adding a rule is one line, and a reviewer can read the whole rule set in one screen. It is the same idea as Delta Live Tables expectations or Great Expectations, done in plain PySpark/SQL as CLAUDE.md asks, with no extra framework to install or explain.
**Detail:** a row fails when its expectation is false **or NULL** (`NOT coalesce(expect, false)`). Otherwise a NULL would quietly count as passing.

### D32. Two severities with tolerances, not pass/fail on everything
- **`ERROR`, 0% tolerance:** things silver guarantees (unique keys, no null amounts, `plan_paid` arithmetic, answer-key reconciliation). A failure means a pipeline bug, so **the job stops** before gold is built on bad data.
- **`WARN`, with a tolerance:** expected source messiness (unknown members ≤ 0.5%, eligibility gaps ≤ 1%, NDC rejects ≤ 2%). Above tolerance it's recorded and flagged to the data owner, but the business still gets its numbers.

**Why:** if every check could stop the job, real feeds would stop it every day and people would start ignoring it. Tolerances encode "what normal looks like". In this data RI04 (1.5% of claims outside eligibility) is above its 1% tolerance and shows as `WARN`. That's the kind of thing a PBM ops team would investigate for recovery.

### D33. Results are appended, and saved before the job fails
**Choice:** `silver.dq_results` is the one silver table that is **appended, not replaced**: one row per check per run, with `run_id`, `run_ts` and the exact SQL that ran. `silver.dq_results_latest` is a view of the newest run for the dashboard. If an `ERROR` check fails, the results are written *first*, then the notebook raises.
**Why:** the history shows trends (is the unknown-member rate creeping up?), and the stored SQL makes every result reproducible. Writing before failing means a broken run still leaves evidence of what broke.

### D34. NPI checksum as a Unity Catalog SQL function
**Choice:** `silver.is_valid_npi(npi)` implements the CMS Luhn rule (prefix `80840`) in pure SQL with `aggregate(sequence(1, 15), ...)`. Before any check uses it, the notebook asserts it gives the right answer on a known-valid and a known-invalid NPI.
**Why:** a governed, reusable function means the DQ check, gold and any ad-hoc dashboard query apply exactly the same rule. A SQL function is used rather than a Python UDF because it runs inside the SQL engine, works the same on the SQL warehouse, and needs no Python environment.

### D35. Two ways to catch bad pharmacy NPIs
`RI03` (NPI not in the pharmacy directory) and `VA06` (NPI fails the checksum) overlap but aren't the same. An NPI can be well-formed but missing from the directory (reference lag), or malformed (keying error). In this data 1,767 claims have an unknown pharmacy and 1,575 of those also fail the checksum. That split tells the data owner which fix is needed.

### D36. The answer-key check is synthetic; the real-world equivalent is a control total
`CP06` reconciles silver to the generator's manifest. Real feeds don't come with an answer key, but they usually come with a **trailer record or control file** (row count and total paid amount per file). In production, `CP06` would compare bronze to those control totals instead.

---

## Step 5 – Gold marts

### D37. Gold is all SQL, one notebook, rebuilt every run
**Choice:** `05_gold_marts.sql` is a SQL notebook. Every mart is `CREATE OR REPLACE TABLE ... AS SELECT` from silver, apart from `pipeline_audit` (D41).
**Why:** gold is aggregation and joins, which SQL expresses most clearly, and analysts who will maintain or extend it read SQL. A full rebuild is quick at this volume, and it inherits silver's guarantees (D27).

### D38. `drug_spend_monthly` at month × drug, with three spend measures
**Choice:** one row per fill month and `drug_name` (NDC package sizes rolled up), carrying `gross_cost`, `member_copay` and `plan_paid`. Out-of-eligibility and unknown-member claims are included and broken out in their own columns.
**Why:** "spend" means different things to different readers. A plan sponsor cares about `plan_paid`. Trend and specialty analysis use `gross_cost`. Showing all three avoids arguments about which number is right. Breaking out out-of-eligibility spend sizes the recovery opportunity without hiding it from totals (D26).
**Result:** specialty is **1.8% of claims and 57.1% of gross cost** ($133.3M of $233.4M).

### D39. Utilisation uses eligible member-months, annualised per 1,000
**Choice:** the denominator is eligible member-months by month × plan. A member counts in a month if their eligibility window overlaps it. The headline metric is `claims ÷ member_months × 12 × 1,000`, plus adjusted scripts (90-day = 3 × 30-day) and gross cost PMPM.
**Why:** a raw member count overstates the denominator when members join and leave mid-period, which makes utilisation look lower than it is. Member-months is the industry standard, and annualising makes months and plans comparable. Adjusted scripts stop a shift to 90-day mail fills from looking like a fall in utilisation.
**Unknown members:** claims for members missing from the member file go to `plan_id = 'UNKNOWN_MEMBER'` with null rates, so claim totals still reconcile to silver.
**Result:** 823,543 member-months, **12,912 claims per 1,000 members per year** (about 12.9 scripts per member per year), **$282.90 PMPM**.

### D40. Rebate matching with one winning contract per claim
**Choice:** brand claims join contracts on manufacturer + effective dates + (NDC match **or** class match). `QUALIFY row_number()` keeps exactly one contract per claim: NDC-level first, then the higher %, then `contract_id`. `estimated_rebate = ingredient_cost × rebate_pct`. `estimated_rebate_if_stacked` records what a naive join summing every match would claim.
**Why:** a naive join double-counts. In this data **20,029 claims match two contracts**, and stacking would overstate rebates by **$0.58M**. Keeping the stacked figure makes that control visible instead of just asserting it.
**Assumption:** real contracts usually apply the % to WAC per unit. With no WAC price list, ingredient cost is the stand-in. That's the first thing to change with real data.
**Result:** 90,659 rebate-eligible claims, **$17.14M** estimated rebate.

### D41. `pipeline_audit`: upsert per run with `MERGE`
**Choice:** one row per (run, table) with row count, plan paid and a note, covering landing → bronze → silver (final, rejected, DQ) → gold. Written with `MERGE ... ON (run_id, table_name)`. The run id comes from the latest `silver.dq_results` run, which in the Job is the same `{{job.run_id}}`.
**Why:** a re-run of gold for the same pipeline run must not duplicate audit rows, while earlier runs must be kept. That's exactly an upsert. Taking the run id from the DQ run that gated gold ties "these numbers were published" to "these checks passed" without passing parameters into a SQL notebook.

### D42. Gold reconciles to silver and stops the job if it doesn't
**Choice:** eight checks (claim counts, gross cost and plan paid tie between gold marts and `silver.claims_final`; rebate claims are unique, brand-only and inside contract dates). `raise_error()` fails the notebook if any check is false or NULL.
**Why:** aggregation bugs, such as a join that drops or multiplies rows, are the most common way a gold number goes wrong without anyone noticing. Exact ties to silver catch them before a dashboard shows the wrong total.

### D43. `top_drugs` is a view, not a table
**Why:** it's a small ranking on top of `drug_spend_monthly`, so a view is always in step with the mart and costs nothing to store. The dashboard filters `rank_by_spend <= 10`.

---

## Step 6 – Orchestration

### D44. One Job, six tasks in a straight line, all serverless
**Choice:** `setup_catalog → generate_synthetic_data → bronze_ingest → silver_claims → silver_dq → gold_marts`. Every task is a notebook task with no cluster definition, so it runs on serverless jobs compute.
**Why:** each layer depends on the one before it, so there's nothing to parallelise, and a straight line is the easiest graph to read in a screenshot. One task at a time also stays well inside Free Edition's 5-concurrent-task limit. Setup and data generation are included so that **one run builds the whole project from an empty workspace**.
**Alternative:** run bronze's five loads as parallel tasks. They take seconds each, so the extra graph complexity isn't worth it.

### D45. Data quality gates gold
**Choice:** `gold_marts` has `run_if: ALL_SUCCESS` (the default, stated explicitly). `04_silver_dq` raises on any `ERROR`-level failure (D32), so gold is **skipped** and the run shows as failed.
**Why:** a dashboard showing yesterday's correct numbers is better than one showing today's wrong numbers. `WARN`-level issues don't stop the run, so expected source messiness never blocks the business.

### D46. Job-level parameters, including `{{job.run_id}}`
**Choice:** three job parameters are pushed to every task: `catalog`, `run_id` (default `{{job.run_id}}`) and `reset` (default `false`). Notebooks that don't use a parameter ignore it.
**Why:** one run id flows into bronze (`_run_id` column), DQ (`dq_results.run_id`) and gold (`pipeline_audit.run_id`), so one run can be traced through every layer. `reset = true` via "Run now with different parameters" does a full bronze reload without editing code.

### D47. Safe to re-run, one run at a time, schedule created paused
- **Re-runs:** the generator is seeded, so every run rewrites identical files. Auto Loader skips paths it has already loaded, so bronze loads 0 new rows. Silver and gold rebuild to identical results, and the audit `MERGE`s on the new run id. A second run proves the pipeline is idempotent.
- **`max_concurrent_runs: 1` + queueing:** two overlapping runs would rebuild the same tables at the same time. A second trigger waits in the queue instead of colliding.
- **Schedule:** daily at 06:00 UTC, but **paused**. It shows how the pipeline would run in production without spending the daily compute quota every day.
- **No retries:** every failure here is a data or logic problem, and an automatic retry would just fail again and hide the first error.

### D48. The JSON is the job's code record
**Choice:** `jobs/pipeline_job.json` holds the full job definition (Jobs API 2.2 format). The job is created in the UI for this project, and the JSON can deploy the same job with `databricks jobs create --json @jobs/pipeline_job.json`.
**Why:** the job is reviewable and repeatable from Git rather than living only as clicks in a workspace. Notebook paths use a `YOUR_USER_NAME` placeholder so no personal workspace details are committed.
**Next step in a real team:** Databricks Asset Bundles (`databricks.yml`), which deploy notebooks and the job together per environment (dev / prod).

---

## Step 7 – Dashboard

### D49. AI/BI dashboard on the Serverless Starter Warehouse
**Choice:** a Databricks AI/BI (Lakeview) dashboard, two pages, running on the Free Edition 2X-Small SQL warehouse.
**Why:** it's included in Free Edition, it sits next to the data under the same Unity Catalog permissions, and it needs no extra tool or licence. A 2X-Small warehouse is enough because every dataset reads a gold aggregate (the largest is ~11.5k rows), not raw claims.
**Alternative:** Power BI or Tableau over a SQL connection. These are more common in PBM shops, but they add a tool and a licence, and the point here is the lakehouse.

### D50. Dataset SQL lives in Git
**Choice:** each dataset is a `.sql` file in `dashboards/datasets/`. The published dashboard is exported to `dashboards/pharmacy_claims_lakehouse.lvdash.json` (its dataset SQL is identical to those files), so it can be re-created in any workspace with one import.
**How it was built:** datasets were added by hand from the SQL files; the widget layout was placed with Genie, the dashboard's AI assistant, from a written spec, then every number was checked against the reconciled gold totals. An earlier attempt to hand-generate the `.lvdash.json` failed to import: the file format is internal and changes (it moved to a 12-column grid and a new dataset schema), so exporting from the product is more reliable than writing the file.
**Why:** dashboard logic is code. A reviewer can read how every number is computed without logging in, and the export re-creates the dashboard in any workspace with "Import dashboard from file".

### D51. Ratios are computed in SQL, not in widgets
**Choice:** PMPM, claims per 1,000 per year and specialty share are calculated in the dataset SQL as `sum(numerator) / sum(denominator)`. Chart datasets expose only additive measures (counts, dollars).
**Why:** the most common dashboard error is averaging ratios: the average of 24 monthly PMPMs is not the period PMPM when member counts change. If only additive columns reach the widgets, no widget can aggregate them wrongly.

### D52. Counter tiles are fixed whole-book totals
**Choice:** the tiles read a one-row `kpi_headline` dataset. Page filters (fill month, therapeutic class, drug type) change the charts but not the tiles.
**Why:** the tiles then always equal the reconciled gold totals, which is what a reviewer will compare against `pipeline_audit`. Making every ratio tile respond to filters would mean moving the ratio logic into widget expressions, and that is exactly what D51 avoids.
**Trade-off:** a user who filters to Oncology sees Oncology charts but whole-book tiles. The page subtitle says so.

### D53. Page 2 shows the data can be trusted
**Choice:** the second page combines rebates with rejects by reason, the latest DQ results and the latest pipeline audit. It reads two silver tables (`claims_rejected`, `dq_results_latest`) on purpose. They are already small, curated exception and audit tables, not raw claims.
**Why:** a hiring manager's first question about any dashboard is "how do I know this is right?". Showing that rejected claims are accounted for, the checks passed and the row counts trace from file to mart answers it on the page. Unmapped NDCs ($1.38M plan paid) also tell the rebate leakage story: claims that can't be matched to a drug can't be invoiced for rebate.

### D54. No scheduled refresh
**Choice:** the dashboard is published with the owner's credentials and no refresh schedule.
**Why:** the data only changes when the pipeline job runs. A schedule would wake the warehouse and spend daily quota to re-read identical numbers. Opening the dashboard runs the queries on demand, and the warehouse auto-stops afterwards.
