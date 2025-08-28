% Azure + Databricks Context

This folder contains generated context snapshots and images used in the case study and runbooks.

## Snapshots
- Source of truth index: `./.context/index.md`
- Azure: `azure_rg_inventory.json`, `storage_account.json`, ADF (`adf_*.json`)
- Databricks: `dbx_clusters.json`, `dbx_jobs.json`, `dbx_secrets_scopes.json`, `dbx_workspace_root.json`

## Screenshots (drop here)
Add PNG/JPG files to `./images/` and the links below will render automatically when present.

- Linked services: `./images/linked_services.png`
- Pipeline canvas: `./images/pipeline.png`
- Monitor (Succeeded): `./images/monitor_success.png`
- Monitor (Failed): `./images/monitor_failed.png`
- Storage RAW file: `./images/storage_raw_sample.png`
- Databricks counts: `./images/databricks_counts.png`
- GitHub Actions success: `./images/actions_success.png`
- dbt fct_taxi_daily query: `./images/dbt_fct_taxi_daily.png`

> Note: These images are optional and can be updated anytime; the context snapshots are refreshed by the `context-harvest` workflow.

