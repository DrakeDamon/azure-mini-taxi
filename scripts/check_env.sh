#!/usr/bin/env bash
set -euo pipefail

REQUIRED=(
  SUBSCRIPTION_ID RESOURCE_GROUP ADF_FACTORY STORAGE_ACCOUNT
  DATABRICKS_HOST DATABRICKS_TOKEN ORG_ID CLUSTER_ID
)

MISSING=()
for v in "${REQUIRED[@]}"; do
  [[ -z "${!v:-}" ]] && MISSING+=("$v")
done

if ((${#MISSING[@]})); then
  echo "❌ Missing vars: ${MISSING[*]}" >&2; exit 1
else
  echo "✅ Env looks good"
fi

