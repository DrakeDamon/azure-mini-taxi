# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

End-to-end Azure data pipeline: Azure Data Factory ingests NYC taxi CSV over HTTP into ADLS Gen2; Databricks notebook processes Bronze → Silver medallion layers; dbt builds Gold aggregated fact table. GitHub Actions handles CI/CD for both Databricks notebooks and dbt models.

## Architecture

Complete medallion architecture with orchestration:
- **Raw Layer**: CSV files ingested by ADF into ADLS (`abfss://taxi@sttaxistorage.dfs.core.windows.net/raw/`)
- **Bronze Layer**: Raw data copied to Delta format (`/delta/bronze/taxis`)  
- **Silver Layer**: Cleaned and transformed data (`/delta/silver/taxis`)
- **Gold Layer**: dbt-built aggregated fact table (`fct_taxi_daily`)

### Data Flow
1. **ADF Pipeline**: Daily HTTP ingestion (06:00 UTC) with failure alerts
2. **Databricks Notebook** (`bronze_silver.ipynb`): Bronze/Silver processing with validation
3. **dbt Models**: Silver → Gold transformation with data quality tests

## Common Commands

### Environment Setup
```bash
make env                    # Check required environment variables
make venv                   # Bootstrap Python 3.11 venv with dbt-databricks
```

### Context Gathering
```bash
make context               # Gather both Azure and Databricks context
make azure                 # Azure resource inventory and ADF status  
make dbx                   # Databricks clusters, jobs, workspace state
```

### dbt Development
```bash
make dbt-debug             # Test dbt connection to Databricks
make dbt-run               # Build dbt models (staging + marts)
make dbt-test              # Run dbt data quality tests
make dbt-docs              # Generate dbt documentation
make dbt-build             # Full build: run + test + docs
```

### Manual Pipeline Execution
- **Databricks**: Run `/Shared/bronze_silver` notebook end-to-end
- **ADF**: Debug pipeline with default parameters in Azure portal

## Configuration

### Databricks Secrets (OAuth for ADLS)
- Secret scope: `adls-oauth`
- Required secrets: `app-id`, `app-secret`, `tenant-id`
- Storage account: `sttaxistorage`, container: `taxi`

### Environment Variables
All configuration in `.env` file. Required variables checked by `scripts/check_env.sh`:
- Azure: `SUBSCRIPTION_ID`, `RESOURCE_GROUP`, `ADF_FACTORY`, `STORAGE_ACCOUNT`
- Databricks: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `ORG_ID`, `CLUSTER_ID`

## Data Quality and Validation

The validation cell in `bronze_silver.ipynb` provides comprehensive health checks:
- RAW file existence and readability
- Bronze/Silver Delta table creation and row counts
- Data quality checks: trip_distance > 0, fare >= 0, payment_type validation
- Returns ✅ PASS or ❌ FAIL with detailed error messages

dbt tests validate:
- Column constraints (not_null on key fields)
- Accepted values for payment_type: ['Cash','Credit','Other']

## CI/CD Workflows

- **Notebook Deploy** (`.github/workflows/deploy.yml`): Imports `bronze_silver.py` to `/Shared/bronze_silver` on push
- **dbt Pipeline** (`.github/workflows/dbt.yml`): Runs dbt build on dbt_taxi changes with artifact upload