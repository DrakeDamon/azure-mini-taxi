.PHONY: context azure dbx
context: azure dbx
azure:
	./scripts/ctx_azure.sh
dbx:
	./scripts/ctx_databricks.sh