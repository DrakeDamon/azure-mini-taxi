#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
#   SUBSCRIPTION_ID, RESOURCE_GROUP, ADF_FACTORY, STORAGE_ACCOUNT
: "${SUBSCRIPTION_ID:?Set SUBSCRIPTION_ID in env}"
: "${RESOURCE_GROUP:?Set RESOURCE_GROUP in env}"
: "${ADF_FACTORY:?Set ADF_FACTORY in env}"
: "${STORAGE_ACCOUNT:?Set STORAGE_ACCOUNT in env}"

OUT="adf-dbx-delta/.context"
mkdir -p "$OUT"

echo "== Ensure extensions =="
az extension add -n resource-graph >/dev/null 2>&1 || az extension update -n resource-graph >/dev/null 2>&1 || true
az extension add -n datafactory     >/dev/null 2>&1 || az extension update -n datafactory     >/dev/null 2>&1 || true

echo "== Azure Resource Graph (RG inventory) =="
az graph query -q "
Resources
| where subscriptionId == '$SUBSCRIPTION_ID'
| where resourceGroup == '$RESOURCE_GROUP'
| project name, type, location, id, tags" -o json > "$OUT/azure_rg_inventory.json"

echo "== Storage account (endpoints & props) =="
az storage account show -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" -o json > "$OUT/storage_account.json"

echo "== ADF: linked services, datasets, pipelines, triggers =="
az datafactory linked-service list -g "$RESOURCE_GROUP" -n "$ADF_FACTORY" -o json > "$OUT/adf_linked_services.json"
az datafactory dataset list         -g "$RESOURCE_GROUP" -n "$ADF_FACTORY" -o json > "$OUT/adf_datasets.json"
az datafactory pipeline list        -g "$RESOURCE_GROUP" -n "$ADF_FACTORY" -o json > "$OUT/adf_pipelines.json"
az datafactory trigger list         -g "$RESOURCE_GROUP" -n "$ADF_FACTORY" -o json > "$OUT/adf_triggers.json"

echo "== ADF: pipeline runs (last 24h) =="
# portable 24h window
if date -u -d "24 hours ago" +%Y-%m-%dT%H:%M:%SZ >/dev/null 2>&1; then
  SINCE=$(date -u -d "24 hours ago" +%Y-%m-%dT%H:%M:%SZ)
else
  SINCE=$(python - <<'PY'
from datetime import datetime,timedelta;print((datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
PY
)
fi
UNTIL=$(date -u +%Y-%m-%dT%H:%M:%SZ)

az datafactory pipeline-run query-by-factory \
  -g "$RESOURCE_GROUP" -n "$ADF_FACTORY" \
  --last-updated-after "$SINCE" --last-updated-before "$UNTIL" -o json \
  > "$OUT/adf_runs_24h.json" || echo "[]">"$OUT/adf_runs_24h.json"

echo "Wrote Azure context to $OUT"

