# Feature parity: databricks-emulator vs. a real Databricks workspace

How the emulator's surface maps to Databricks' public REST (Workspace, Jobs,
Unity Catalog), and — the point of this table — **whether real work happens or
just the API shape**.

A row is 🟢 **Real** only when an unmodified client drove the call and the
attached engine or store actually did the work. Status without a witness is
not support. See [00-doctrine.md](00-doctrine.md).

**Witnessed claims: 17.** See [witnesses.json](witnesses.json).

## Identity

| Surface | What would make it real | Status |
|---|---|---|
| Identity — PAT | Seeded/minted PAT; unknown / `dev` is 401 | 🟢 Real |
| Identity — emulator OIDC | Client-credentials → `Me` with no entra process | 🟢 Real |
| Identity — federated JWT | Opt-in issuer list; unconfigured / wrong aud / expired is 401 | 🟢 Real |

## Workspace

| Surface | What would make it real | Status |
|---|---|---|
| Workspace — SOURCE/PYTHON | Classic `/workspace/import` SOURCE/PYTHON notebook round-trip | 🟢 Real |
| Workspace — raw files | workspace-files raw bytes, including `RAW`/`FILE` import | 🟢 Real |
| DBFS / Files API | Real bytes on a blob store | 🟢 Real |

## Jobs

| Surface | What would make it real | Status |
|---|---|---|
| Jobs 2.2 — notebook / Python | Attached Spark engine executes the file | 🟢 Real (engine attached) |
| Jobs 2.2 — JAR / dbt / DLT / sql_task.query | Refused at create | 🔴 refuse |

## Secrets

| Surface | What would make it real | Status |
|---|---|---|
| Secrets — Databricks injection | `{{secrets}}` in job env and `spark_conf`; GET rejected; missing fails the run | 🟢 Real |
| Secrets — Databricks persist | Survive process restart under `data/secrets/` | 🟢 Real |
| Secrets — Azure Key Vault-backed | Live read-through at use time; `put`/`delete` refused; rotate the vault secret and the next run sees it | 🟢 Real (vault attached) |
| Secrets — vault-audience token | Entra client-credentials at `https://vault.azure.net/.default` sent on the vault GET | 🟢 Real (entra attached) |

## Compute

| Surface | What would make it real | Status |
|---|---|---|
| SQL warehouses | Spark SQL, dialect named in the output | 🟢 Real (engine attached) |
| Clusters as VMs | — | 🔴 refuse |
| Clusters as session handle | Create starts a Sail session | 🟢 Real (engine attached) |
| Databricks Connect | Spark Connect | 🟢 Real (engine attached) |

## Catalog

| Surface | What would make it real | Status |
|---|---|---|
| Unity Catalog CRUD | UC OSS sidecar | 🟢 Real (sidecar attached) |
| Unity Catalog grants | Enforcement, not allow-all CRUD | 🔴 refuse (not shipped until they deny) |

## Clients

| Surface | What would make it real | Status |
|---|---|---|
| MCP — Databricks SQL | Same SQL warehouse handler, behind PAT/OIDC | 🟢 Real (engine attached) |
| Terraform / DAB pair | Unmodified `databricks/databricks`: current_user + notebook + workspace_file + job; `token=dev` refused | 🟢 Real |

## Refused

| Surface | What would make it real | Status |
|---|---|---|
| MCP — Genie / AI Search / UC functions | — | 🔴 refuse |
| Photon / DBR compatibility | — | 🔴 refuse |
| Lakeflow / DLT | — | 🔴 refuse |
| Model Serving / Vector Search / Dashboards | — | 🔴 refuse |
