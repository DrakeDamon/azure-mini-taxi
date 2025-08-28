.PHONY: context azure dbx env

context: azure dbx

azure:
	./scripts/ctx_azure.sh

dbx:
	./scripts/ctx_databricks.sh

env:
	chmod +x scripts/*.sh || true
	set -a; source .env; set +a; ./scripts/check_env.sh

