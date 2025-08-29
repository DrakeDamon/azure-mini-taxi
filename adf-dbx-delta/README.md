# Azure + Databricks Context

This folder contains generated context snapshots and images used in the case study and runbooks.

## Snapshots
- Source of truth index: `./.context/index.md`
- Azure: `azure_rg_inventory.json`, `storage_account.json`, ADF (`adf_*.json`)
- Databricks: `dbx_clusters.json`, `dbx_jobs.json`, `dbx_secrets_scopes.json`, `dbx_workspace_root.json`

## Screenshots (drop here)
Add PNG/JPG files to `./images/` using these names:

- Pipeline failed (ADF): `./images/adf_pipeline_failed.png`
- Alert rule fired (portal): `./images/alert_fired.png`
- Alert email: `./images/alert_email.png`
- (optional) Notebook error: `./images/dbx_notebook_error.png`
- (optional) Pipeline succeeded (ADF): `./images/adf_pipeline_succeeded.png`

> Note: These images are optional; the context snapshots are refreshed by the `context-harvest` workflow.
