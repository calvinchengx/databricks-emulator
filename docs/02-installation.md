# 02 — Installation

A single static Go binary (no CGO). Pick a channel that exists; this repo
does not yet ship Homebrew or winget.

## From source (recommended while developing)

```bash
git clone https://github.com/calvinchengx/databricks-emulator
cd databricks-emulator
make doctor
make build          # ./databricks-emulator
make run            # https://localhost:8447
```

`make doctor` checks for Go and [uv](https://docs.astral.sh/uv/) (the same
Python toolchain as fabric-emulator). `make e2e` / `e2e-engine` / `e2e-delta`
/ `e2e-uc` / `e2e-dbt-uc` run through `uv run --frozen --group …` against `pyproject.toml`
and `uv.lock`. Docker is required for the engine, Delta, UC, and dbt attaches.
`databricks-connect==19.1` needs Python 3.12; uv reads `.python-version`.

## Docker / GHCR

```bash
docker pull ghcr.io/calvinchengx/databricks-emulator:0.2.3
docker run --rm -p 8447:8447 \
  -e DATABRICKS_DISABLE_TLS=1 \
  ghcr.io/calvinchengx/databricks-emulator:0.2.3
```

The image is distroless. Its `HEALTHCHECK` runs the binary's own
`healthcheck` subcommand (no shell in the container), which pins the cert
this process already serves — see [TLS and hosts](05-tls-and-hosts.md).

Persist state by mounting `/data` (`DATABRICKS_DATA_DIR=/data` is set in the
image). The first start writes `admin.pat` there.

## Family compose

This repo has no root compose file. The family stack lives in
[azure-emulators](https://github.com/calvinchengx/azure-emulators):

```bash
docker compose --profile databricks up   # :8447, plus entra :8443 and keyvault :8444
```

That profile sets federated Entra and a live vault host. It does **not** set
`DATABRICKS_SPARK_CONNECT_URL`. Identity and secret *resolve* work; job
*execution* still needs Sail — [Jobs and the Spark attach](08-jobs-and-spark.md).

## go install

```bash
go install github.com/calvinchengx/databricks-emulator/cmd/databricks-emulator@latest
```

## Release binaries

Prebuilt archives for linux/darwin/windows × amd64/arm64 are attached to each
[GitHub release](https://github.com/calvinchengx/databricks-emulator/releases).

`databricks-emulator version` prints the build; `healthcheck` probes `/health`
and exits 0 when live. Full settings in [Configuration](04-configuration.md).
