# ADF → ADLS → Databricks (Delta) + Schedule + Alerts

End‑to‑end Azure data pipeline: Azure Data Factory ingests a taxi CSV over HTTP into ADLS Gen2; a Databricks notebook writes Delta Bronze → Silver and dbt builds a daily Gold fact. GitHub Actions handles notebook import CI; optional ADF ARM deploy fits in the same pipeline.

## Architecture
Diagram source: `design/diagram.mmd`

```mermaid
%% Diagram mirrors design/diagram.mmd
flowchart LR
  A[HTTP taxis.csv] -->|ADF Copy| B[(ADLS Gen2: taxi/raw)]
  B -->|Databricks Notebook| C[(Delta: bronze/taxis)]
  C --> D[(Delta: silver/taxis)]
  D --> E[(Delta: gold/taxi_daily)]
  A -.-> G[ADF Trigger (daily 06:00)]
  A -.-> H[Alert rule: Failed pipeline runs → Email]
  C -.-> I[dbt: fct_taxi_daily]
```

## Medallion
- Bronze: raw CSV landed to `delta/bronze/taxis`.
- Silver: typed/cleansed with normalized `payment_type`.
- Gold: `fct_taxi_daily` aggregated by date and payment.

## What This Proves
- Orchestration: ADF Copy + scheduled trigger + alert rule.
- Storage/Compute: ADLS Gen2 + Databricks + Delta tables.
- CI/CD: Action imports `/Shared/bronze_silver` on push; ADF ARM optional.
- Data Quality: strict/warn toggle; resilient RAW filename handling.

## Runbook (re‑run quickly)
- ADF: Debug pipeline (parameters below) → succeeds.
- Databricks: run `/Shared/bronze_silver` end‑to‑end (Bronze/Silver → PASS/WARN).
- dbt: `cd dbt_taxi && dbt run && dbt test` builds `fct_taxi_daily`.

Parameters (ADF defaults)
- `source_url`: `https://raw.githubusercontent.com/mwaskom/seaborn-data/master/taxis.csv`
- `date_str`: `@{formatDateTime(utcnow(),'yyyyMMdd')}`

## Screenshots (relative paths)
- images/adf_linked_services.png
- images/adf_pipeline_canvas.png
- images/adf_pipeline_failed.png
- images/adf_pipeline_succeeded.png
- images/adf_trigger_daily.png
- images/adls_raw_file.png
- images/dbx_bronze_silver_gold_counts.png
- images/gha_deploy_success.png
- images/alert_email.png
- images/dbt_fct_taxi_daily_query.png

## Resources
- Data Factory: `adf-taxi-sono-global`
- Storage: `sttaxistorage` (container: `taxi`, folders: `raw/`, `delta/`)
- Notebook: `/Shared/bronze_silver`
- Repo path: `azure-mini-taxi/adf-dbx-delta`
