# Dashboard – Pharmacy Claims Lakehouse

A two-page **Databricks AI/BI dashboard** on the gold layer, running on the Free Edition **Serverless Starter Warehouse** (2X-Small).

| Page | Question it answers | Built on |
|---|---|---|
| **1. Spend & utilisation** | Where is the money going, and how much do members use? | `gold.drug_spend_monthly`, `gold.top_drugs`, `gold.utilisation` |
| **2. Rebates & data trust** | What rebate is the plan owed, what is being missed, and can these numbers be trusted? | `gold.rebate_eligible_claims`, `silver.claims_rejected`, `silver.dq_results_latest`, `gold.pipeline_audit` |

Page 1 tells the business story. Page 2 shows the numbers are earned: every claim is either in the marts or in the rejects with a reason, 34 data quality checks ran, and the row counts trace from the landing files to gold.

All data is synthetic. Drug and manufacturer names are fictional.

---

## Datasets

Each dataset is one SQL file in [`datasets/`](datasets/). They live in Git, so the dashboard's logic can be reviewed without opening Databricks.

| # | Dataset name | File | Rows | Used by |
|---|---|---|---|---|
| 1 | `kpi_headline` | `01_kpi_headline.sql` | 1 | Counter tiles on both pages |
| 2 | `spend_monthly` | `02_spend_monthly.sql` | ~11.5k | Spend trend, class chart, page 1 filters |
| 3 | `top_drugs` | `03_top_drugs.sql` | 10 | Top 10 table |
| 4 | `utilisation_monthly` | `04_utilisation_monthly.sql` | 24 | PMPM trend |
| 5 | `utilisation_by_plan` | `05_utilisation_by_plan.sql` | ~6 | PMPM by plan |
| 6 | `rebates_monthly` | `06_rebates_monthly.sql` | small | Rebate trend |
| 7 | `rejected_claims` | `07_rejected_claims.sql` | 3 | Rejects by reason |
| 8 | `dq_latest` | `08_dq_latest.sql` | 34 | DQ status chart and table |
| 9 | `pipeline_audit_latest` | `09_pipeline_audit_latest.sql` | 8 | Audit table |

**Rules the SQL follows:**
- **Ratios are computed in SQL, never in a widget.** PMPM, claims per 1,000 and specialty share are each one sum divided by another. Letting a chart average monthly or per-plan ratios would give a wrong answer.
- **Chart datasets expose only additive measures** (counts and dollars), so any `SUM` a widget does is correct.
- **Counter tiles are whole-book totals** from `kpi_headline`. Page filters change the charts, not the tiles. This keeps the tiles matching the gold reconciliation.

---

## Layout

```
PAGE 1 · Spend & utilisation
┌──────────────────────────────────────────────────────────────────────────┐
│ Title text                                       [Fill month] [Class] [Type] │
├───────────┬───────────┬───────────┬───────────┬───────────┬──────────────┤
│ Claims    │ Gross $M  │ Spec % $  │ Spec % #  │ PMPM $    │ Claims/1k/yr │
├───────────┴───────────┴───────────┼───────────┴───────────┴──────────────┤
│ A. Monthly gross cost (area,      │ B. Gross cost by therapeutic class   │
│    specialty vs traditional)      │    (bar, coloured by drug type)      │
├───────────────────────────────────┼──────────────────────────────────────┤
│ C. Top 10 drugs by spend (table)  │ D. Gross cost PMPM by month (line)   │
│                                   ├──────────────────────────────────────┤
│                                   │ E. PMPM by plan (bar)                │
└───────────────────────────────────┴──────────────────────────────────────┘

PAGE 2 · Rebates & data trust
┌──────────────────────────────────────────────────────────────────────────┐
│ Title text                                                               │
├────────────────┬────────────────┬────────────────┬───────────────────────┤
│ Rebate $M │ Rebate claims │ Double count $M │ Unmapped NDC $M │ Rejected │ Plan paid │
├────────────────┴────────────────┼────────────────┴───────────────────────┤
│ F. Estimated rebate by month    │ G. Rejected claims by reason (bar)     │
│    (stacked by contract level)  │                                        │
├─────────────────────────────────┼────────────────────────────────────────┤
│ H. DQ checks by status (pie)    │ J. Pipeline audit, latest run (table)  │
├─────────────────────────────────┴────────────────────────────────────────┤
│ I. Data quality checks, latest run (table)                               │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Quick route: import the dashboard file (about 2 minutes)

[`pharmacy_claims_lakehouse.lvdash.json`](pharmacy_claims_lakehouse.lvdash.json) is the published dashboard, exported from Databricks. It contains all 9 datasets (their SQL is identical to the files in `datasets/`) and both pages.

1. Left sidebar → **Dashboards**.
2. Click the **▾** arrow on the **Create dashboard** button → **Import dashboard from file**.
3. Choose `pharmacy_claims_lakehouse.lvdash.json` from this folder → **Import dashboard**.
4. The draft opens. If it asks for a warehouse, choose **Serverless Starter Warehouse**.
5. Check the numbers against **Step 5** below, then **Publish** (Step 6).

The datasets read `portfolio.gold` / `portfolio.silver`, so the pipeline job must have run in the target workspace first.

---

## Manual route: build it click by click

How the dashboard was built. Use it to rebuild from scratch or to learn the editor. The page layouts in the published version were placed with Genie (the dashboard's AI assistant) from the widget specs below, then checked against Step 5. Takes about 30–40 minutes by hand. The dashboard autosaves as a draft while you work.

### Step 1 – Create the dashboard
1. Left sidebar → **Dashboards** → **Create dashboard** (top right).
2. Click the title **New Dashboard** at the top and rename it to `Pharmacy Claims Lakehouse`.

### Step 2 – Add the 9 datasets
For each file in `datasets/`, in order:
1. Click the **Data** tab (top left of the draft).
2. Click **+ Add SQL dataset**. A new dashboard already has one empty "Untitled dataset"; use that for the first file.
3. Paste the file's contents into the editor and click **Run**. Check the result looks like the "Rows" column in the table above.
4. Rename the dataset: in the Datasets list, click **⋮** next to "Untitled dataset" → **Rename**, and use the name from the table (e.g. `kpi_headline`).

The catalog and schema dropdowns above the editor ("Default (workspace)", "Default (default)") can stay as they are: every query names its tables in full (`portfolio.gold...`). If a warehouse picker appears, choose **Serverless Starter Warehouse**.

**Check on `kpi_headline`** (single row):

| Column | Expected |
|---|---|
| total_claims | 887,876 |
| gross_cost_musd | 233.4 |
| plan_paid_musd | 214.0 |
| specialty_pct_of_spend | 57.1 |
| specialty_pct_of_claims | 1.8 |
| member_months | 823,543 |
| claims_per_1000_per_year | 12,912 |
| gross_cost_pmpm | 282.90 |
| rebate_claims | 90,659 |
| estimated_rebate_musd | 17.14 |
| double_count_avoided_musd | 0.58 |
| rejected_claims | 14,022 |
| unmapped_ndc_plan_paid_musd | 1.38 |

### Step 3 – Page 1 widgets
Click the **Canvas** tab. The toolbar at the bottom of the canvas has buttons to add a **visualization**, a **text box** and a **filter**. Add a widget, then drag its edges to place and size it as in the layout above. Settings appear in the panel on the right when a widget is selected.

Rename the page: double-click the page name **Untitled page** → `Spend & utilisation`.

**Title (text box):**
```
## Pharmacy Claims Lakehouse
Synthetic PBM claims, Jan 2024 – Dec 2025 · medallion pipeline on Databricks Free Edition · gold layer
Tiles show whole-book totals; the filters apply to the charts.
```

**Six counters.** For each: add a visualization → **Dataset** `kpi_headline` → **Visualization** `Counter` → **Value** = the field below (aggregation `SUM` is fine, there's one row) → turn on **Title** and type the title.

| Value field | Title |
|---|---|
| `total_claims` | Final paid claims |
| `gross_cost_musd` | Gross cost ($M) |
| `specialty_pct_of_spend` | Specialty % of spend |
| `specialty_pct_of_claims` | Specialty % of claims |
| `gross_cost_pmpm` | Gross cost PMPM ($) |
| `claims_per_1000_per_year` | Claims per 1,000 members / yr |

**Charts:**

| Widget | Dataset | Visualization | Settings | Title |
|---|---|---|---|---|
| A | `spend_monthly` | Area | X: `fill_month` (scale Temporal, **Monthly**) · Y: `SUM(gross_cost)` · Color: `drug_type` | Monthly gross cost: specialty vs traditional |
| B | `spend_monthly` | Bar | X: `therapeutic_class` · Y: `SUM(gross_cost)` · Color: `drug_type` · sort X by Y descending | Gross cost by therapeutic class |
| C | `top_drugs` | Table | Columns: `spend_rank`, `drug_name`, `therapeutic_class`, `drug_type`, `claims`, `gross_cost_musd`, `cost_per_claim` | Top 10 drugs by spend, latest year |
| D | `utilisation_monthly` | Line | X: `fill_month` (Monthly) · Y: `SUM(gross_cost_pmpm)` (one row per month, so SUM = the value) | Gross cost PMPM by month |
| E | `utilisation_by_plan` | Bar | X: `plan_id` · Y: `SUM(gross_cost_pmpm)` | Gross cost PMPM by plan |

**Filters** (add a filter → place it top right of the page):

| Filter type | Field(s) | Title |
|---|---|---|
| Date range picker | `spend_monthly.fill_month` **and** `utilisation_monthly.fill_month` (click **+** next to Fields to add the second) | Fill month |
| Multiple values | `spend_monthly.therapeutic_class` | Therapeutic class |
| Multiple values | `spend_monthly.drug_type` | Drug type |

### Step 4 – Page 2 widgets
Add a page with the **+** next to the page name, and call it `Rebates & data trust`.

**Title (text box):**
```
## Rebates & data trust
Rebate = ingredient cost × contract % (one contract per claim: NDC-level beats class-level). Every number on this dashboard reconciles to silver; checks run on every pipeline run.
```

**Six counters** (dataset `kpi_headline`, Counter):

| Value field | Title |
|---|---|
| `estimated_rebate_musd` | Estimated rebate ($M) |
| `rebate_claims` | Rebate-eligible claims |
| `double_count_avoided_musd` | Double count avoided ($M) |
| `unmapped_ndc_plan_paid_musd` | Plan paid on unmapped NDCs ($M) |
| `rejected_claims` | Rejected claim rows |
| `plan_paid_musd` | Plan paid ($M) |

**Charts and tables:**

| Widget | Dataset | Visualization | Settings | Title |
|---|---|---|---|---|
| F | `rebates_monthly` | Bar | X: `fill_month` (Monthly) · Y: `SUM(estimated_rebate)` · Color: `contract_level` · stacked | Estimated rebate by month |
| G | `rejected_claims` | Bar | X: `reason` · Y: `SUM(rejected_claims)` · switch to horizontal if labels are cramped | Rejected claims by reason |
| H | `dq_latest` | Pie | Angle: `COUNT(*)` · Color: `status` | Data quality checks, latest run |
| I | `dq_latest` | Table | Columns: `check_id`, `category`, `check_name`, `severity`, `status`, `failed_rows`, `total_rows`, `failed_pct`, `tolerance_pct` | Data quality checks (non-PASS first) |
| J | `pipeline_audit_latest` | Table | Columns: `layer`, `table_name`, `row_count`, `plan_paid`, `note` | Pipeline audit, latest run |

### Step 5 – Check the numbers

| Widget | You should see |
|---|---|
| Counters, page 1 | 887,876 · 233.4 · 57.1 · 1.8 · 282.90 · 12,912 |
| A | Two bands over 24 months. The specialty band is larger than traditional every month |
| C | #1 Praxilia 200 MG TAB (Oncology, specialty): 530 claims, ≈ $9.49M, ≈ $17,900 per claim. #3 Exadra 10 MG TAB (traditional): 10,627 claims at ≈ $627 per claim |
| Counters, page 2 | 17.14 · 90,659 · 0.58 · 1.38 · 14,022 · 214.0 |
| F | Mostly NDC-level (≈ $16.5M of $17.1M); class-level is a thin band |
| G | INVALID_NDC_FORMAT 7,363 · NDC_NOT_IN_REFERENCE 6,409 · ORPHAN_REVERSAL 250 |
| H | 33 PASS, 1 WARN |
| I | First row is RI04 (eligibility), WARN at 1.50% |
| J | 8 rows; the gold rows show 887,876 claims and $213,985,787.66 plan paid, the same as `silver.claims_final` |

Try a filter: pick **Oncology** in Therapeutic class. Charts A and B change; the counters stay the same (by design, see above).

### Step 6 – Publish
1. Top right → **Publish**. Keep the default credential option (the dashboard runs as you, the owner) → **Publish**.
2. Click **View published** to see the read-only version a viewer would get.

No refresh schedule is set. The data changes only when the pipeline job runs, and a schedule would use warehouse quota for nothing.

### Step 7 – Save to Git and screenshot
1. After any change, export a fresh copy and save it over `dashboards/pharmacy_claims_lakehouse.lvdash.json`, so Git holds the published version. Export is in the **draft** view (**Edit draft** → **⋮**) or in the Workspace browser (**⋮** on the dashboard file → Export). The published view's menu only offers PDF. Anyone can re-create the dashboard from it with **Dashboards → Create dashboard ▾ → Import dashboard from file**.
2. Screenshot each published page and save as `docs/images/dashboard_page1.png` and `docs/images/dashboard_page2.png`. Crop out your email, workspace URL and avatar first.
