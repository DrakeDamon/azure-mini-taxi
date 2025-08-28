#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
#   DATABRICKS_HOST, DATABRICKS_TOKEN
: "${DATABRICKS_HOST:?Set DATABRICKS_HOST in env}"
: "${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN in env}"

OUT="adf-dbx-delta/.context"
mkdir -p "$OUT"

echo "== Databricks workspace inventory =="
databricks clusters list            --output JSON > "$OUT/dbx_clusters.json" || echo "[]">"$OUT/dbx_clusters.json"
databricks jobs list                --output JSON > "$OUT/dbx_jobs.json"     || echo "[]">"$OUT/dbx_jobs.json"
databricks secrets list-scopes      --output JSON > "$OUT/dbx_secrets_scopes.json" || echo "[]">"$OUT/dbx_secrets_scopes.json"
databricks workspace list --absolute --output JSON --path "/" > "$OUT/dbx_workspace_root.json" || echo "[]">"$OUT/dbx_workspace_root.json"

echo "# Context snapshots
See index.md for what's here." > "$OUT/README.md"
echo "Wrote Databricks context to $OUT"

