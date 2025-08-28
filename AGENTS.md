# Repository Guidelines

## Project Structure & Module Organization
- `adf-dbx-delta/notebooks/bronze_silver.py`: Databricks notebook (PySpark) that reads RAW CSV from ADLS and writes Delta BRONZE/SILVER; includes validation and Hive table registration.
- `bronze_silver.ipynb`: Interactive version of the pipeline for authoring/debugging.
- `dbt_taxi/`: dbt project for analytics.
  - `dbt_project.yml`, `profiles.yml` (Databricks adapter).
  - `models/staging/silver_taxis.sql` (staging model from SILVER table).
  - `models/marts/fct_taxi_daily.sql` (daily fact over trips).
- `ARMTemplateForFactory.json`, `ARMTemplateParametersForFactory.json`: Azure Data Factory templates.

## Build, Test, and Development Commands
- Setup dbt (Databricks): `cd dbt_taxi && export DATABRICKS_TOKEN=*** && dbt debug --profiles-dir .`
- Run models: `dbt run --profiles-dir .`
- Build (run + test): `dbt build --profiles-dir .`
- Test only: `dbt test --profiles-dir .`
- Notebook validation: run the first “Validation” cell in `adf-dbx-delta/notebooks/bronze_silver.py` (or `bronze_silver.ipynb`) in Databricks; expect `✅ PASS` summary.

## Coding Style & Naming Conventions
- SQL: snake_case columns (e.g., `pickup_ts`, `fare_amount`); stage models in `models/staging`, marts in `models/marts`; fact models prefixed `fct_`.
- PySpark notebooks: PEP 8, 4‑space indent, descriptive names; never hard‑code secrets—use `dbutils.secrets`.
- Filenames: staging models use domain names (e.g., `silver_taxis.sql`); marts use `fct_*`.

## Testing Guidelines
- dbt tests: co-locate YAML tests next to models (e.g., `models/staging/schema.yml`). Minimum: `not_null` for keys and `accepted_values` for `payment_type`. Run with `dbt test`.
- Data quality: keep notebook checks green; add new checks before writing SILVER.

## Commit & Pull Request Guidelines
- Commits: imperative and concise (e.g., `Add Silver taxis model`, `Refactor fct_taxi_daily`).
- PRs: clear description, linked issues, validation screenshot (PASS output), and any schema changes. Exclude credentials and secret material from diffs/logs.

## Security & Configuration Tips
- Auth: Databricks token via `DATABRICKS_TOKEN` (referenced by `profiles.yml`); Azure secrets via scope `adls-oauth`. Do not commit tokens; keep `.env` local.
- Storage: default catalog/schema `hive_metastore.default`; ADLS paths like `abfss://taxi@sttaxistorage.dfs.core.windows.net/(raw|delta/...)`.

