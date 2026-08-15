# databricks-emulator

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![CI](https://github.com/calvinchengx/databricks-emulator/actions/workflows/ci.yml/badge.svg)](https://github.com/calvinchengx/databricks-emulator/actions/workflows/ci.yml)
[![Docs](https://github.com/calvinchengx/databricks-emulator/actions/workflows/docs-site.yml/badge.svg)](https://calvinchengx.github.io/databricks-emulator/)

A clean-room, local emulator of a **Databricks workspace**, built as a peer of
[fabric-emulator](https://github.com/calvinchengx/fabric-emulator) and the rest
of the [Azure emulator family](https://github.com/calvinchengx/azure-emulators).

The bet is the same one the family already runs: **terminate the public REST,
attach a real engine, refuse what you cannot compute.**

Identity is Databricks-native: a **seeded admin PAT** and this process's own
**OIDC** (`/oidc/v1/token`). Entra is an optional federated issuer
(`DATABRICKS_OIDC_ISSUERS`). `token=dev` is 401.

```bash
make doctor
make test
make witnesses
make run   # https://localhost:8447 — first run prints the admin PAT once
```

Point the official SDK at it:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host="https://localhost:8447", token=open("data/admin.pat").read().strip())
print(w.current_user.me().user_name)
```

TLS is on by default (self-signed). `DATABRICKS_DISABLE_TLS=1` serves HTTP.
Jobs need an attached statement agent: `DATABRICKS_SPARK_CONNECT_URL`. Without
one, `run-now` fails naming the missing engine — never `SUCCESS`.

Secret scopes are Databricks-backed by default (persisted under `data/secrets/`).
`AZURE_KEYVAULT` scopes are a **live read-through** of a vault — set
`DATABRICKS_AKV_VAULT_HOST` to allow keyvault-emulator. `put`/`delete` on those
scopes are refused; rotate the vault secret and the next job run sees the new
value. There is no sync. Family compose with entra sets
`DATABRICKS_ENTRA_TOKEN_URL` so the resolve carries a vault-audience bearer;
without it, `make run` still works against a stand-in vault.

SQL warehouses are a session handle onto the same Spark agent — not a VM and
not Photon. `POST /api/2.0/sql/statements` runs Spark SQL; the response names
`dialect: spark-sql`. `sql_task.file` jobs take the same path.
`sql_task.query` / dashboard / alert stay refused.

Clusters are a session handle onto that same engine — not a VM.
`POST /api/2.0/clusters/create` starts a Sail session or fails naming the
missing engine; it never sleeps to `RUNNING`. Databricks Connect is Spark
Connect gRPC reverse-proxied to `DATABRICKS_SPARK_CONNECT_GRPC_URL` (Sail
`:50051`) after PAT/OIDC and a `x-databricks-cluster-id` that names a
RUNNING handle. The HTTP statement agent is not that backend. Autoscale and
cluster libraries stay refused.

Unity Catalog CRUD reverse-proxies to a [UC OSS](https://github.com/unitycatalog/unitycatalog)
sidecar (`DATABRICKS_UC_URL`). Without one those routes are 501 naming the
missing sidecar. MANAGED table create is refused (UC OSS only creates
EXTERNAL tables at a filesystem location). Grants stay 501 until they deny.

`POST /api/2.0/mcp/sql` is the Databricks SQL MCP server: `execute_sql` /
`poll_response` wrap the warehouse statements handler after PAT/OIDC.
Genie, AI Search, and UC function MCP paths stay 501.

The official Terraform provider (`databricks/databricks`) applies a notebook,
a workspace file, and a job against the seeded PAT — the DAB pair. `token=dev`
is refused. `make e2e-terraform` is the witness.

`make e2e-engine` attaches Sail + the family's spark-agent (and entra +
keyvault) and drives cluster create, unmodified `databricks-connect`
`SELECT 1`, a Python job whose logs contain `REACHED`, `{{secrets}}`
printed from `os.environ`, an AKV-backed scope whose rotate is visible on
the next run, a SQL warehouse `SELECT 1` that names `dialect: spark-sql`,
and MCP `execute_sql`. Run it with `uv` (`.python-version` is 3.12).

`make e2e-delta` writes a Delta table through the warehouse API onto Sail
and confirms `_delta_log` and the rows with **delta-rs**, not with Sail.

`make e2e-uc` attaches [UC OSS](https://github.com/unitycatalog/unitycatalog)
`v0.5.0` and drives catalog / schema / EXTERNAL table create through the
unmodified SDK. MANAGED create and grants stay 501.

Unmapped `/api/*` is **501** `NOT_IMPLEMENTED`, never a silent 200.

See the [docs site](https://calvinchengx.github.io/databricks-emulator/)
([quickstart](docs/01-quickstart.md), [doctrine](docs/00-doctrine.md),
[parity ledger](docs/parity.md)). The ledger's catalog is the
[workspace REST API reference](https://docs.databricks.com/api/workspace/);
green rows are unmodified clients, not doc pages.

## Emulator family

| Repo | Role |
|---|---|
| [entra-emulator](https://github.com/calvinchengx/entra-emulator) | Optional federated STS |
| [azure-keyvault-emulator](https://github.com/calvinchengx/azure-keyvault-emulator) | Key Vault data plane |
| [arm-emulator](https://github.com/calvinchengx/arm-emulator) | ARM control plane + RBAC |
| [azure-apim-emulator](https://github.com/calvinchengx/azure-apim-emulator) | API Management |
| [fabric-emulator](https://github.com/calvinchengx/fabric-emulator) | Fabric. Set `FABRIC_DATABRICKS_URL` to consume this workspace |
| **databricks-emulator** | Databricks workspace |

## License

Apache-2.0. Clean-room: grounded solely in Databricks' public
[workspace REST API reference](https://docs.databricks.com/api/workspace/)
(Workspace, Jobs, Unity Catalog), with Databricks' own SDK and CLI as
the conformance oracle — no Databricks Runtime source.
