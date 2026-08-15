# Everyday verbs for databricks-emulator. No compose yet — identity is
# Databricks-native; entra is an optional federated issuer.

ifeq ($(OS),Windows_NT)
  SHELL := sh.exe
  .SHELLFLAGS := -c
endif

PY ?= $(shell for c in python3 python py; do if "$$c" -c '' >/dev/null 2>&1; then echo "$$c"; break; fi; done)

.PHONY: help doctor build run test e2e e2e-terraform clean witnesses

help: ## Show the available targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'

doctor: ## Check the toolchain
	@command -v go >/dev/null || { echo "go is required" >&2; exit 1; }
	@go version

build: ## Compile the binary
	go build -o databricks-emulator ./cmd/databricks-emulator

run: build ## Serve natively (DATABRICKS_DISABLE_TLS=1 for plain HTTP)
	./databricks-emulator

test: ## go build, vet and unit tests
	go build ./... && go vet ./... && go test ./...

e2e: ## Unmodified databricks-sdk against a local server
	@test -n "$(PY)" || { echo "no working python found; set PY=" >&2; exit 1; }
	$(PY) -m pip install -q -r e2e/sdk/requirements.txt
	$(PY) e2e/sdk/run.py

e2e-terraform: ## Unmodified databricks/databricks provider against a local server
	@test -n "$(PY)" || { echo "no working python found; set PY=" >&2; exit 1; }
	@command -v terraform >/dev/null || { echo "terraform is required on PATH" >&2; exit 1; }
	$(PY) e2e/terraform/run.py

witnesses: ## Verify docs/witnesses.json points at real tests
	@test -n "$(PY)" || { echo "no working python found; set PY=" >&2; exit 1; }
	$(PY) scripts/check_witnesses.py

clean: ## Remove the built binary and ./data
	rm -f databricks-emulator databricks-emulator.exe
	rm -rf data
