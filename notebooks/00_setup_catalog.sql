-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Set up the `portfolio` catalog
-- MAGIC
-- MAGIC Creates the Unity Catalog objects every later notebook depends on:
-- MAGIC
-- MAGIC | Object | Purpose |
-- MAGIC |---|---|
-- MAGIC | `portfolio` | One catalog for the whole project (Free Edition has a single metastore). |
-- MAGIC | `portfolio.raw` | Holds the landing **volume** only, with no tables. Keeps files apart from tables. |
-- MAGIC | `portfolio.raw.landing` | Managed volume where the generator drops CSV extracts (`/Volumes/portfolio/raw/landing`). |
-- MAGIC | `portfolio.raw.checkpoints` | Managed volume for Auto Loader checkpoints and schema state (bronze loads). |
-- MAGIC | `portfolio.bronze` | Raw files loaded as-is into Delta, plus `_ingested_at` / `_source_file`. |
-- MAGIC | `portfolio.silver` | Cleansed, conformed, deduplicated claims plus DQ results. |
-- MAGIC | `portfolio.gold` | Business marts for the dashboard. |
-- MAGIC
-- MAGIC **Idempotent:** everything uses `IF NOT EXISTS`, so re-running is safe and never drops data.
-- MAGIC (There is no `CREATE OR REPLACE SCHEMA`. Replacing a schema would mean dropping its tables, which a setup script should never do.)
-- MAGIC
-- MAGIC **Free Edition notes**
-- MAGIC * No `MANAGED LOCATION` / storage credentials: Free Edition gives us Databricks-managed default storage, so all objects are *managed*.
-- MAGIC * If `CREATE CATALOG` is refused in your workspace, use the workspace catalog that already exists instead. Replace `portfolio` below
-- MAGIC   and pass the same name to the `catalog` widget in `01_generate_synthetic_data.py`.

-- COMMAND ----------

-- MAGIC %md ## 1. Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS portfolio
COMMENT 'Pharmacy Claims Lakehouse portfolio project - synthetic data only, no PHI.';

-- COMMAND ----------

USE CATALOG portfolio;

-- COMMAND ----------

-- MAGIC %md ## 2. Schemas (medallion layers + raw landing zone)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Landing zone. Contains the `landing` volume for source files; no tables.';

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze: source files loaded as-is into Delta, all columns as strings, with _ingested_at and _source_file lineage.';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver: NDC normalised to ndc11, reversals netted, duplicates removed, eligibility flagged, DQ results.';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold: business marts (drug spend, utilisation, rebate-eligible claims, pipeline audit).';

-- COMMAND ----------

-- MAGIC %md ## 3. Landing volume
-- MAGIC A **managed** volume: Unity Catalog governs the files and they live in default storage, so no cloud credentials are needed.
-- MAGIC The generator writes one sub-folder per source entity (`claims/`, `members/`, ...) so each bronze load can point at its own folder.

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS raw.landing
COMMENT 'Synthetic source extracts (CSV) written by 01_generate_synthetic_data. One sub-folder per entity.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A second managed volume for **Auto Loader state** (checkpoints and inferred schemas), kept apart from the source files
-- MAGIC so that clearing or re-generating landing files never touches pipeline state, and the reverse. Added in Step 2.

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS raw.checkpoints
COMMENT 'Auto Loader checkpoints and schema tracking for bronze loads. Pipeline state, not data.';

-- COMMAND ----------

-- MAGIC %md ## 4. Verify

-- COMMAND ----------

SHOW SCHEMAS IN portfolio;

-- COMMAND ----------

SHOW VOLUMES IN portfolio.raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reset (optional, commented out on purpose)
-- MAGIC Uncomment to wipe the whole project and start again. This drops every table and the landing files.

-- COMMAND ----------

-- DROP CATALOG IF EXISTS portfolio CASCADE;
