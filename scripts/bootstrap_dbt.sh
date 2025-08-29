#!/usr/bin/env bash
set -euo pipefail

echo "== bootstrap: Python 3.11 venv + dbt-databricks 1.8.x =="

# Find Python 3.11
if command -v /opt/homebrew/bin/python3.11 >/dev/null 2>&1; then
  PY311=/opt/homebrew/bin/python3.11
elif command -v python3.11 >/dev/null 2>&1; then
  PY311=$(command -v python3.11)
else
  echo "Python 3.11 not found. On macOS: brew install python@3.11" >&2
  exit 1
fi

echo "Using: $PY311"

$PY311 -m venv .venv
echo "Created .venv/"

source .venv/bin/activate
python -m pip install --upgrade pip
pip install "dbt-databricks==1.8.*"

echo
echo "✅ dbt installed. Next steps:"
echo "1) source .venv/bin/activate"
echo "2) export DATABRICKS_TOKEN=\"<your_PAT>\" (or set in ~/.zshrc)"
echo "3) cd dbt_taxi && dbt debug --profiles-dir . && dbt run --profiles-dir ."

