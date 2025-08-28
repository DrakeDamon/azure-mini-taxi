# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data engineering project implementing a medallion architecture (Bronze → Silver) for NYC taxi data processing. The project uses PySpark and Delta Lake to process CSV taxi data from Azure Data Lake Storage (ADLS).

## Architecture

- **Raw Layer**: CSV files in ADLS (`abfss://taxi@sttaxistorage.dfs.core.windows.net/raw/`)
- **Bronze Layer**: Raw data copied to Delta format (`/delta/bronze/taxis`)  
- **Silver Layer**: Cleaned and transformed data (`/delta/silver/taxis`)

The main notebook `bronze_silver.ipynb` contains:
1. ADLS OAuth configuration using Databricks secrets
2. Data validation and quality checks
3. Bronze layer creation (direct copy from raw CSV)
4. Silver layer transformation with schema changes and data cleaning
5. Hive metastore table registration

## Data Flow

**Bronze**: Direct copy of raw CSV data to Delta format
**Silver**: Schema transformation including:
- Column renaming (pickup → pickup_ts, dropoff → dropoff_ts, etc.)
- Type casting (distance, fare, tip, tolls to double)
- Payment type standardization (cash/credit capitalized, others → "Other")
- Null value filtering on trip_distance and fare_amount

## Configuration

The project uses Databricks secrets for Azure authentication:
- Secret scope: `adls-oauth`
- Required secrets: `app-id`, `app-secret`, `tenant-id`
- Storage account: `sttaxistorage`
- Container: `taxi`

## Running the Pipeline

Execute cells sequentially in `bronze_silver.ipynb`. The validation cell (cell 1) provides comprehensive health checks and will show ✅ PASS or ❌ FAIL with detailed error messages.

Data quality checks validate:
- trip_distance > 0
- fare >= 0  
- payment type in allowed values (cash, credit)