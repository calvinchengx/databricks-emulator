# Feature parity: databricks-emulator vs. a real Databricks workspace

How the emulator's surface maps to Databricks' public workspace REST, and —
the point of this table — **whether real work happens or just the API shape**.

The **catalog** is the [workspace REST API reference](https://docs.databricks.com/api/workspace/)
(sidebar retrieved 2026-08-15). That list is what real Databricks offers at
the workspace host. Product pages, DBR version strings, and third-party
OpenAPI scrapes are not a denominator.

The design bet is the same one [fabric-emulator](https://github.com/calvinchengx/fabric-emulator)
runs: **terminate the public contract here, attach a real engine, refuse what
you cannot compute.** This process owns identity, files, secrets persist, and
the workspace REST shim. Jobs, SQL, clusters, Connect, and UC CRUD are
answered by a named sidecar — Sail behind the family's spark-agent, Spark
Connect gRPC, UC OSS, keyvault-emulator — never by a toy stub or a silent
DuckDB-as-Photon.

`make run` does not start those sidecars. `make e2e-engine` / `make e2e-uc`
do. Family compose does **not** set the Spark URLs. A missing engine fails
naming the variable — never `SUCCESS` / `RUNNING`.

The **wire** is proved by an unmodified client: `databricks-sdk`, the
Databricks CLI, or `databricks/databricks` Terraform. A doc page that names
an endpoint is not support. See [00-doctrine.md](00-doctrine.md).

## Legend

| | Meaning |
|---|---|
| 🟢 **Real** | The shim terminates the contract and a named store or engine did the work. An unmodified client drove the call. Status without a witness is not support. |
| 🟡 **Emulated** | Faithful API contract + persisted state, but no engine — status is clock-derived / management-only. |
| 🟠 **Non-default engine** | Real, but only on an engine that is *not* the default attach (Sail + spark-agent). Not a silent substitute: the row names the overlay. |
| 🔴 **Not implemented** | Honest 501 or absent. Never a silent 200. |

## Scope boundary: the workspace REST, not Databricks Runtime

This ledger grades **workspace API groups** from the reference, written in
fabric's style: **feature → what the shim terminates → which engine
computes**. It is not every operation inside a group, and not the
account-level APIs at
[docs.databricks.com/api/account](https://docs.databricks.com/api/account/).
A green Jobs row is not every Jobs endpoint; a red Pipelines row is the
whole Lakeflow/DLT group.

The first honest slice is identity (PAT + emulator OIDC), Workspace files,
DBFS, Jobs 2.2 Python/notebook on Sail, secrets, SQL warehouses / Connect /
clusters-as-session on that same engine, and Unity Catalog CRUD through UC
OSS. Everything else from the catalog is enumerated below as Not
implemented until a witness exists.

This is not a Databricks Runtime. Photon, DBR version strings, full
`dbutils` / `spark.databricks.*`, cluster VMs, and any "DBR compatible"
claim stay out even though the docs mention them. Crossing that line
would change the project's character.

## Identity

| Feature | Emulator | Type |
|---|---|---|
| Identity — PAT | This process seeds and mints PATs. `GET /api/2.0/preview/scim/v2/Me` after `Authorization: Bearer`. Unknown and `token=dev` are 401 — `dev` is MiniLake's trap, not a credential this seeder mints. | 🟢 Real |
| Identity — emulator OIDC | This process's `/oidc/v1/token` client-credentials. `Me` with no entra process. | 🟢 Real |
| Identity — federated JWT | Opt-in `DATABRICKS_OIDC_ISSUERS`. Unconfigured / wrong aud / expired / garbage → 401; a good JWT → `Me`. Entra is an issuer, not a required STS. | 🟢 Real |

## Workspace

| Feature | Emulator | Type |
|---|---|---|
| Workspace — SOURCE/PYTHON | File-backed store. Classic `/workspace/import` SOURCE/PYTHON notebook round-trip. Other formats refused by name. | 🟢 Real |
| Workspace — raw files | workspace-files raw bytes, including `RAW`/`FILE` import. | 🟢 Real |
| DBFS / Files API | Real bytes under `data/dbfs/`. Read length capped (1 MiB) before allocate. Traversal refused. | 🟢 Real |

## Jobs

| Feature | Emulator | Type |
|---|---|---|
| Jobs 2.2 — notebook / Python | Shim resolves workspace/DBFS file, bakes argv / notebook params / `{{secrets}}` into a Python preamble (`os.environ.update`), POSTs `{agent}/statements` with `kind: python`. Without `DATABRICKS_SPARK_CONNECT_URL`, `run-now` fails naming the engine — never `SUCCESS`. Default attach is Sail behind the family's spark-agent (`make e2e-engine`). Family compose does not set the URL. | 🟢 Real |
| Jobs 2.2 — JAR / dbt / DLT / sql_task.query | Refused at create. `sql_task.file` takes the warehouse Spark SQL path (see SQL warehouses). No JVM overlay is shipped, so JAR is not 🟠. | 🔴 Not implemented |

## Secrets

| Feature | Emulator | Type |
|---|---|---|
| Secrets — Databricks injection | Shim resolves `{{secrets/scope/key}}` in this process before the engine runs. GET of a value is 400. Missing key → run `FAILED`. The family's spark-agent drops `req.Env`, so the preamble bakes `os.environ.update`. Witness prints `SECRET=s3cret` in `get-output`. | 🟢 Real |
| Secrets — Databricks persist | Databricks-backed scopes under `data/secrets/`. Survive process restart with the same `DATABRICKS_DATA_DIR`. | 🟢 Real |
| Secrets — Azure Key Vault-backed | Live read-through at use time — no sync. `dns_name` must be an Azure suffix or `DATABRICKS_AKV_VAULT_HOST`. `put`/`delete` refused. Rotate the vault secret; the next run GETs it. | 🟢 Real |
| Secrets — vault-audience token | When `DATABRICKS_ENTRA_TOKEN_URL` is set, each vault GET carries a client-credentials bearer with scope `https://vault.azure.net/.default`. Empty URL: resolve stays unauthenticated (stand-in / `make run`). | 🟢 Real |

## Compute

| Feature | Emulator | Type |
|---|---|---|
| SQL warehouses | Session handle, not a VM and not Photon. `POST /api/2.0/sql/statements` sends the SQL as `kind: sql` (the code **is** Spark SQL). Wire names `dialect: spark-sql`; `executedBy` says Spark SQL, not Photon. Without `DATABRICKS_SPARK_CONNECT_URL`, execute is `FAILED` naming the engine. | 🟢 Real |
| Delta writes — Sail | Warehouse SQL `CREATE TABLE … USING delta LOCATION` + `INSERT` + `DELETE` + `MERGE INTO` on a shared volume. Sail writes; **delta-rs** reads `_delta_log` and the rows. A Sail `COUNT(*)` after DML is not a witness. Standalone `UPDATE` is forwarded and Sail answers FAILED (`CommandNode::Update`) — never a silent no-op. Photon is not this row. | 🟢 Real |
| Delta writes — UC three-part names | SDK creates an EXTERNAL table in UC OSS (Docker sidecar). Sail's unity catalog provider (`SAIL_CATALOG__LIST`, same Compose network) resolves `cat.sch.tbl`. Warehouse `INSERT` writes; **delta-rs** confirms the log. This process forwards the name unchanged — it is not a path rewrite. | 🟢 Real |
| Clusters as session handle | `POST /api/2.0/clusters/create` starts a Sail session (`print(1)` via the HTTP agent) or fails naming the missing engine. Never sleeps to `RUNNING`. Autoscale and cluster libraries stay refused. | 🟢 Real |
| Databricks Connect | After PAT/OIDC and `x-databricks-cluster-id` naming a RUNNING handle, `application/grpc` / `/spark.connect.…` is reverse-proxied to `DATABRICKS_SPARK_CONNECT_GRPC_URL` (Sail `:50051`, h2c). The HTTP agent is not this backend; only that URL set is 501 naming the gRPC variable. Authorization stripped before the engine. | 🟢 Real |
| Clusters as VMs | No hypervisor. A session handle is not a VM. | 🔴 Not implemented |
| Photon / DBR compatibility | No Photon attach exists. Sail is Spark SQL over Spark Connect. Relabeling it (or DuckDB) as Photon is the lookalike doctrine refuses. | 🔴 Not implemented |

## Catalog

| Feature | Emulator | Type |
|---|---|---|
| Unity Catalog CRUD | Reverse-proxy to UC OSS (`DATABRICKS_UC_URL`) after PAT/OIDC. Without a sidecar those routes are 501 naming the missing URL. MANAGED table create is refused (UC OSS only creates EXTERNAL tables at a filesystem location). Three-part SQL against those tables is the Delta writes row, not this one. | 🟢 Real |
| Unity Catalog grants | Enforcement, not allow-all CRUD. Not shipped until they deny. | 🔴 Not implemented |

## Clients

| Feature | Emulator | Type |
|---|---|---|
| MCP — Databricks SQL | `POST /api/2.0/mcp/sql` JSON-RPC after PAT/OIDC. `execute_sql` / `poll_response` wrap the warehouse statements handler (same Sail attach, same `dialect: spark-sql`). Genie / AI Search / UC function MCP paths stay 501. | 🟢 Real |
| Terraform / DAB pair | Unmodified `databricks/databricks`: current_user + notebook + workspace_file + job create. `token=dev` refused. Job *execution* is the engine row, not this one. | 🟢 Real |

## Published APIs outside this slice

One row per remaining group on the [workspace REST API reference](https://docs.databricks.com/api/workspace/)
sidebar. The emulator column is what an honest attach would have to be —
same style as the greens — not a product-page promise.

| Feature | Emulator | Type |
|---|---|---|
| Git Credentials / Repos | Would need an unmodified CLI/SDK clone and commit against a real git remote. | 🔴 Not implemented |
| Cluster Policies / Policy Families / compliance | Would need enforcement, not stored-and-ignored. | 🔴 Not implemented |
| Command Execution | Would need a context-id session that runs on the attached Sail agent. | 🔴 Not implemented |
| Global Init Scripts | Applied to a real cluster VM — we have no VMs. | 🔴 Not implemented |
| Instance Pools / Instance Profiles | Real VMs / cloud instance profiles. | 🔴 Not implemented |
| Managed Libraries | Installed on a real cluster VM. JARs on Sail have no classloader (fabric's JVM overlay is the 🟠 path; this repo does not ship one). | 🔴 Not implemented |
| MLflow Experiments / Model Registry | Would need a tracking store + registry, not a 200 stub. | 🔴 Not implemented |
| Apps | Would need a deployed app process. | 🔴 Not implemented |
| SCIM Groups / Users / Service Principals | Directory mutations that then deny. | 🔴 Not implemented |
| Permissions | Enforcement, not allow-all. | 🔴 Not implemented |
| SQL Alerts / Queries / Query History | Stored queries that execute on the warehouse (same Sail attach as statements). | 🔴 Not implemented |
| Unity Catalog beyond CRUD | Volumes, functions, locations, credentials, monitors — sidecar must speak them. | 🔴 Not implemented |
| Delta Sharing | Providers / Recipients / Shares against a real share. | 🔴 Not implemented |
| Marketplace | Consumer + provider listing APIs. | 🔴 Not implemented |
| Token management / Workspace Conf / Settings | Persist and then gate. Token create is not a seeded PAT. | 🔴 Not implemented |
| Clean Rooms | — | 🔴 Not implemented |
| Database Instances / Postgres | Lakebase. Would attach a real Postgres (fabric's SQL Server sidecar pattern), not a handle that reports RUNNING. | 🔴 Not implemented |
| Knowledge Assistants / Supervisor Agents | — | 🔴 Not implemented |
| Lakeview Embedded / AI Gateway | — | 🔴 Not implemented |
| Notification Destinations | — | 🔴 Not implemented |
| MCP — Genie / AI Search / UC functions | Other MCP mounts. SQL MCP is the green row above. | 🔴 Not implemented |
| Lakeflow / DLT | Pipelines API. No open DLT engine to attach. | 🔴 Not implemented |
| Model Serving | Serving endpoints. Would need a real model process, not a 200 stub. | 🔴 Not implemented |
| Vector Search | Indexes / endpoints. Would need a real index engine. | 🔴 Not implemented |
| Dashboards | Lakeview. No dashboard renderer is attached. | 🔴 Not implemented |
