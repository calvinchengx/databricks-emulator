# Everyday verbs for databricks-emulator. Identity is Databricks-native;
# entra is an optional federated issuer. Engine e2e attaches Sail; UC e2e
# attaches Unity Catalog OSS.
#
# Python e2e uses uv + the root pyproject.toml / uv.lock, same as
# fabric-emulator. Do not add requirements.txt.

ifeq ($(OS),Windows_NT)
  SHELL := sh.exe
  .SHELLFLAGS := -c
endif

UV ?= uv
# uv first, matching fabric-emulator: the project environment is the only
# interpreter the witnesses run against. Bare python remains for stdlib
# scripts on a machine that has not installed uv yet.
PY ?= $(shell if command -v uv >/dev/null 2>&1; then echo "uv run --frozen --no-sync python"; \
	else for c in python3 python py; do if "$$c" -c '' >/dev/null 2>&1; then echo "$$c"; break; fi; done; fi)

.PHONY: help doctor build run test e2e e2e-cli e2e-terraform e2e-engine e2e-delta e2e-delta-jvm e2e-uc e2e-sql e2e-databricks-target e2e-dbt e2e-dbt-task e2e-dbt-uc clean witnesses

help: ## Show the available targets
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'

doctor: ## Check the toolchain
	@command -v go >/dev/null || { echo "go is required" >&2; exit 1; }
	@go version
	@command -v $(UV) >/dev/null || { echo "uv is required for e2e (https://docs.astral.sh/uv/); same toolchain as fabric-emulator" >&2; exit 1; }
	@$(UV) --version

build: ## Compile the binary
	go build -o databricks-emulator ./cmd/databricks-emulator

run: build ## Serve natively (DATABRICKS_DISABLE_TLS=1 for plain HTTP)
	./databricks-emulator

test: ## go build, vet and unit tests
	go build ./... && go vet ./... && go test ./...

e2e: ## Unmodified databricks-sdk against a local server
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	$(UV) run --frozen --group sdk python e2e/sdk/run.py

e2e-cli: ## Unmodified Databricks CLI v1.12.1 against a local server
	@test -n "$(PY)" || { echo "no working python found; set PY=" >&2; exit 1; }
	$(PY) e2e/cli/run.py

e2e-terraform: ## Unmodified databricks/databricks provider against a local server
	@test -n "$(PY)" || { echo "no working python found; set PY=" >&2; exit 1; }
	@command -v terraform >/dev/null || { echo "terraform is required on PATH" >&2; exit 1; }
	$(PY) e2e/terraform/run.py

e2e-engine: ## Unmodified databricks-sdk + databricks-connect with Sail attached
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group engine python e2e/engine/run.py

e2e-delta: ## Warehouse SQL writes Delta via Sail; delta-rs confirms the log
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group delta python e2e/delta/run.py

e2e-delta-jvm: ## Warehouse SQL writes Delta via JVM Spark; delta-rs confirms
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group delta python e2e/delta-jvm/run.py

e2e-uc: ## Unmodified databricks-sdk Unity Catalog CRUD with UC OSS attached
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group sdk python e2e/uc/run.py

e2e-sql: ## Unmodified databricks-sql-connector over HiveServer2 Thrift
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group sql python e2e/sql/run.py

e2e-databricks-target: ## databricks-target resolver: warehouse by name + SELECT 1
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group target pytest python/databricks-target/tests -q
	$(UV) run --frozen --group target python e2e/databricks-target/run.py
e2e-dbt: ## Unmodified dbt-databricks table model over HiveServer2
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group dbt python e2e/dbt/run.py
e2e-dbt-task: ## dbt as a Jobs dbt_task, run inside the statement agent
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	# The `delta` group, deliberately: it carries no dbt, so a pass means dbt
	# ran on the AGENT. Run `uv sync --group delta --exact` first if an earlier
	# `--group dbt` run left one in the shared venv -- the suite refuses to
	# start rather than report a witness it cannot make.
	$(UV) run --frozen --group delta python e2e/dbt-task/run.py
e2e-dbt-uc: ## Unmodified dbt-databricks against a Unity Catalog catalog
	@command -v $(UV) >/dev/null || { echo "uv is required" >&2; exit 1; }
	@command -v docker >/dev/null || { echo "docker is required on PATH" >&2; exit 1; }
	$(UV) run --frozen --group dbt python e2e/dbt-uc/run.py

witnesses: ## Verify docs/witnesses.json points at real tests
	@test -n "$(PY)" || { echo "no working python found; set PY=" >&2; exit 1; }
	$(PY) scripts/check_witnesses.py

clean: ## Remove the built binary and ./data
	rm -f databricks-emulator databricks-emulator.exe
	rm -rf data
