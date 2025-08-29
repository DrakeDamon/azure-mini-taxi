.PHONY: context azure dbx env venv dbt-debug dbt-run dbt-test dbt-docs dbt-build

context: azure dbx

azure:
	./scripts/ctx_azure.sh

dbx:
	./scripts/ctx_databricks.sh

env:
	chmod +x scripts/*.sh || true
	set -a; source .env; set +a; ./scripts/check_env.sh

# --- dbt helpers ---
venv:
	bash scripts/bootstrap_dbt.sh

dbt-debug:
	. .venv/bin/activate && cd dbt_taxi && dbt debug --profiles-dir .

dbt-run:
	. .venv/bin/activate && cd dbt_taxi && dbt run --profiles-dir .

dbt-test:
	. .venv/bin/activate && cd dbt_taxi && dbt test --profiles-dir .

dbt-docs:
	. .venv/bin/activate && cd dbt_taxi && dbt docs generate --profiles-dir .

dbt-build:
	. .venv/bin/activate && cd dbt_taxi && dbt build --profiles-dir .
