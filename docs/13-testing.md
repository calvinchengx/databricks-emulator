# 13 — Testing

The coverage number describes `go test`. What catches consumer-facing defects
is the witness fleet — unmodified clients over a real network — which no
percentage scores. Both run in CI.

## What the project verifies

| Command | What it is |
|---|---|
| `make test` | `go build`, `go vet`, `go test`. CI also enforces an 85% coverage floor. |
| `make witnesses` | `scripts/check_witnesses.py` — every 🟢 row in [parity.md](parity.md) names an existing CI job or Go test in [witnesses.json](witnesses.json). |
| `make e2e` | Unmodified `databricks-sdk==0.129.0` (`e2e/sdk/run.py`, CI job `e2e-sdk`). |
| `make e2e-terraform` | Unmodified `databricks/databricks` provider (`e2e/terraform/run.py`, CI job `e2e-terraform`). |
| `make e2e-engine` | Same SDK, with Sail + spark-agent attached (`e2e/engine/run.py`, CI job `e2e-engine`). Needs Docker. |

Pin the SDK. A floating `pip install databricks-sdk` is not a witness — it is
whatever PyPI shipped the morning CI ran.

## Witness kinds

| kind | what it means |
|---|---|
| `ci:<job>` | a CI job driving a real external client |
| `go:<Test>` | a Go test: real HTTP, real store, this repo's client on both ends |

`ci:` is stronger. The family evidence table counts each green claim once, by
its strongest witness. Do not file a `ci:` on a claim the stranger did not
drive.

## What each CI job actually drives

**`e2e-sdk`** — PAT + `token=dev` 401; emulator OIDC M2M `Me`; federated JWT
doors (unconfigured / wrong aud / expired / garbage → 401, good JWT → `Me`);
workspace AUTO upload/download; DBFS put/read; secrets persist across restart;
cluster-create **without** an engine must fail naming
`DATABRICKS_SPARK_CONNECT_URL`.

**`e2e-terraform`** — PAT / `token=dev`; notebook; workspace file; job
**create** (not execution). That is the DAB pair.

**`e2e-engine`** — cluster session `RUNNING`; Python job logs contain
`REACHED`; `{{secrets}}` printed from `os.environ`; AKV rotate visible on
the next run; warehouse `SELECT 1` names `dialect: spark-sql`; MCP
`execute_sql`. Sets `DATABRICKS_SPARK_CONNECT_GRPC_URL` so Connect is not
501, but does not drive `databricks-connect` — that row stays `go:`.

## Not a witness

- A Go test that reports SUCCESS from a scripted hook without the production
  agent. Those tests prove routing and refusal; they do not replace `ci:e2e-engine`.
- `curl` in a README.
- A doc page that names an endpoint.

The family chain test in azure-emulators is a **seam** check on published
images, not this repo's Jobs witness. See
[Family integration](14-family-integration.md).
