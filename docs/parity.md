# Feature parity: databricks-emulator vs. a real Databricks workspace

How the emulator's surface maps to Databricks' public workspace REST, and —
the point of this table — **whether real work happens or just the API shape**.

The **catalog** is the [workspace REST API reference](https://docs.databricks.com/api/workspace/)
(sidebar retrieved 2026-08-15). That list is what real Databricks offers at
the workspace host. Product pages, DBR version strings, and third-party
OpenAPI scrapes are not a denominator.

The **wire** is proved by an unmodified client: `databricks-sdk`, the
Databricks CLI, or `databricks/databricks` Terraform. A doc page that names
an endpoint is not support. See [00-doctrine.md](00-doctrine.md).

## Legend

| | Meaning |
|---|---|
| 🟢 **Real** | An unmodified client drove the call and the attached engine or store did the work. Status without a witness is not support. |
| 🔴 **refuse** | Enumerated absence. The route is 501 `NOT_IMPLEMENTED`, never a silent 200. |

## Scope boundary: the workspace REST, not Databricks Runtime

This ledger grades **API groups** from the workspace reference, not every
operation inside them, and not the account-level APIs at
[docs.databricks.com/api/account](https://docs.databricks.com/api/account/).
A green Jobs row is not every Jobs endpoint; a red Pipelines row is the
whole Lakeflow/DLT group.

The first honest slice is identity (PAT + emulator OIDC), Workspace files,
DBFS, Jobs 2.2 Python/notebook on an attached Spark engine, secrets,
SQL warehouses / Connect / clusters-as-session on that same engine, and
Unity Catalog CRUD through UC OSS. Everything else from the catalog is
enumerated below as refuse until a witness exists.

This is not a Databricks Runtime. Photon, DBR version strings, full
`dbutils` / `spark.databricks.*`, cluster VMs, and any "DBR compatible"
claim stay out even though the docs mention them. Crossing that line
would change the project's character.

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

## Published APIs outside this slice

One row per remaining group on the [workspace REST API reference](https://docs.databricks.com/api/workspace/)
sidebar. These are the catalog entries the first slice does not cover.

| Surface | What would make it real | Status |
|---|---|---|
| Git Credentials / Repos | Unmodified CLI/SDK clone and commit against a real git remote | 🔴 refuse |
| Cluster Policies / Policy Families / compliance | Enforcement, not stored-and-ignored | 🔴 refuse |
| Command Execution | Context-id session that runs on the attached engine | 🔴 refuse |
| Global Init Scripts | Applied to a real cluster VM — we have no VMs | 🔴 refuse |
| Instance Pools / Instance Profiles | Real VMs / cloud instance profiles | 🔴 refuse |
| Managed Libraries | Installed on a real cluster | 🔴 refuse |
| MLflow Experiments / Model Registry | Tracking store + registry, not a 200 stub | 🔴 refuse |
| Apps | Deployed app process | 🔴 refuse |
| SCIM Groups / Users / Service Principals | Directory mutations that then deny | 🔴 refuse |
| Permissions | Enforcement, not allow-all | 🔴 refuse |
| SQL Alerts / Queries / Query History | Stored queries that execute on the warehouse | 🔴 refuse |
| Unity Catalog beyond CRUD | Volumes, functions, locations, credentials, monitors — sidecar must speak them | 🔴 refuse |
| Delta Sharing | Providers / Recipients / Shares against a real share | 🔴 refuse |
| Marketplace | Consumer + provider listing APIs | 🔴 refuse |
| Token management / Workspace Conf / Settings | Persist and then gate; Token create is not a seeded PAT | 🔴 refuse |
| Clean Rooms | — | 🔴 refuse |
| Database Instances / Postgres | Attached postgres, not a handle | 🔴 refuse |
| Knowledge Assistants / Supervisor Agents | — | 🔴 refuse |
| Lakeview Embedded / AI Gateway | — | 🔴 refuse |
| Notification Destinations | — | 🔴 refuse |
| MCP — Genie / AI Search / UC functions | — | 🔴 refuse |
| Photon / DBR compatibility | — | 🔴 refuse |
| Lakeflow / DLT | Pipelines API | 🔴 refuse |
| Model Serving / Vector Search / Dashboards | Serving endpoints, Vector Search indexes/endpoints, Lakeview | 🔴 refuse |
