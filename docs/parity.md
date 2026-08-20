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
DBFS, Git Credentials / Repos, Jobs 2.2 Python/notebook on Sail, secrets, SQL warehouses / Queries / Connect /
clusters-as-session on that same engine, Command Execution, Unity Catalog
CRUD through UC OSS, and MLflow experiments / model registry on the
file-backed tracking store. Everything else from the catalog is enumerated below as Not
implemented until a witness exists.

This is not a Databricks Runtime. Photon, DBR version strings, full
`dbutils` / `spark.databricks.*`, cluster VMs, and any "DBR compatible"
claim are 🔴 Not implemented even though the docs mention them.

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
| Git Credentials / Repos | Git credentials persist (token never returned after create). `repos.create` **git clone**s a real remote into the workspace store; `repos.update` fetch/checkout; workspace export of a cloned file is those bytes. Sparse checkout is 501. | 🟢 Real |
| DBFS / Files API | Real bytes under `data/dbfs/`. Read length capped (1 MiB) before allocate. Traversal refused. | 🟢 Real |

## Jobs

| Feature | Emulator | Type |
|---|---|---|
| Jobs 2.2 — notebook / Python | Shim resolves workspace/DBFS file, bakes argv / notebook params / `{{secrets}}` into a Python preamble (`os.environ.update`), POSTs `{agent}/statements` with `kind: python`. Without `DATABRICKS_SPARK_CONNECT_URL`, `run-now` fails naming the engine — never `SUCCESS`. Default attach is Sail behind the family's spark-agent (`make e2e-engine`). Family compose does not set the URL. **Boundary, until the agent digest bumps: argv and env are per-task only for tasks that do not overlap.** The preamble writes `sys.argv` and `os.environ`, one object each per interpreter, and `runJob` dispatches a wave concurrently — so two parameterised tasks in one wave overwrite each other and both read the winner's, reporting `SUCCESS` either way. Both witnesses below run tasks serially and cannot see it. Fixed agent-side in fabric-emulator (`python/spark_agent/task_scope.py`); `make e2e-task-parameters` is the witness and is red against the pinned 4.2.0 until `SPARK_CLIENT_DIGEST` moves. | 🟢 Real |
| Jobs 2.2 — if/else `condition_task` | Decided in this process, not on the engine: it is pure comparison, and it is evaluated with no Spark attached at all. `depends_on.outcome` selects the arm, so the branch not taken is `SKIPPED` and never reaches the engine. The witness asserts what Spark *saw*, not what the run reported. Operator families follow Databricks' documented split: `EQUAL_TO`/`NOT_EQUAL` compare as **strings** (`12.0 == 12` is false), the four ordering ops as **numbers** (`12.0 >= 12` is true). Ops outside `ConditionTaskOp` and outcomes outside `true`/`false` are refused at create. `ci:e2e-condition-task` runs both families on identical operands through the unmodified SDK and reads the chosen arm back with delta-rs. | 🟢 Real |
| Jobs 2.2 — `dbt_task` | The project is read from the workspace store, carried INLINE to the statement agent (the store is this process's disk; the agent is another container, so a path meaningful here is meaningless there), and dbt is invoked through `dbtRunner` against a generated profile pointing at the `warehouse_id` the task named. dbt is an ordinary warehouse client, so running it as a job changes who invokes it, not what it connects to. `source: GIT` is refused by name: clone with the Repos API and point `project_directory` at the result. An unknown warehouse, a directory with no `dbt_project.yml`, and a project over 8 MiB all fail naming the cause. Needs `dbt-databricks` on the agent; an older agent fails saying exactly that. | 🟢 Real |
| Jobs 2.2 — JAR / DLT / sql_task.query | Refused at create. `sql_task.file` takes the warehouse Spark SQL path (see SQL warehouses). No JVM overlay is shipped, so JAR is not 🟠. | 🔴 Not implemented |
| Jobs 2.2 — `for_each_task` / `run_job_task` | **Implemented.** Both are pure orchestration this process can compute, which is why they were named as a gap rather than a boundary. `run_job_task` runs the named job **synchronously** — the parent task is not done until the child is, which is what `depends_on` downstream of it has to mean — and reports the child's outcome as its own, with the child's id on `run_job_output.run_id` so a caller can fetch what actually ran. `for_each_task` runs its nested task once per input, honouring `concurrency` (default serial), substituting `{{input}}` into the iteration's parameters and reporting `task_run_stats`. **Three refusals kept, because each would otherwise be a silent no-op or a hang**: `inputs` that decode to an empty list (a loop over nothing that reports SUCCESS), a `for_each_task` or `run_job_task` nested inside a loop, and `run_job_task` nesting deeper than 5 — a job that runs itself, directly or through a cycle, would not terminate. `inputs` is decoded at **create** time, not run time, so a malformed one is refused where the mistake was made. **Witnessed by the unmodified `databricks-sdk` with delta-rs reading the result** (`ci:e2e-composite-tasks`): three inputs leave three Delta tables, each holding its own value — the only way to tell three iterations of one value from three of different ones — and the child job's table exists only because the child really ran, since the parent writes nothing itself. **`concurrency` > 1 is witnessed too**, and the suite runs at `concurrency=len(inputs)` rather than serial: at 1 it would pass either way. The earlier limitation ([#64](https://github.com/calvinchengx/databricks-emulator/issues/64)) was never in this repository — the statement agent shared `sys.argv` across sessions, so every iteration wrote the same table and the surplus writers died on a Delta commit conflict while the run reported SUCCESS. Fixed in the agent by fabric-emulator#338 and shipped in v0.32.0; the pinned digest here is the first to carry it. | 🟢 Real |
| Jobs 2.2 — `python_wheel_task` | Refused by name: installing a wheel means owning a cluster's lifecycle, which this process does not. Same boundary as `libraries`. | 🔴 Not implemented |

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
| SQL Queries / Query History | Stored query CRUD (`/api/2.0/sql/queries`). Execute is the warehouse statements path with that `query_text` (same Sail attach). History lists those executions. Alerts and visualizations stay 501. | 🟢 Real |
| SQL warehouses — Thrift / HiveServer2 | Same warehouse handle and Sail attach. `POST /sql/1.0/endpoints/{id}` (and `/sql/protocolv1/o/{org}/{id}` when `{id}` is a warehouse) is TBinary HiveServer2. Unmodified `databricks-sql-connector==4.4.0` `SELECT 1` returns one typed cell. GetSchemas / GetTables are `SHOW` on that engine. Cloud Fetch / Arrow+LZ4 / GetCatalogs stay refused. Missing engine fails naming `DATABRICKS_SPARK_CONNECT_URL`. | 🟢 Real |
| Delta writes — Sail | Warehouse SQL `CREATE TABLE … USING delta LOCATION` + `INSERT` + `DELETE` + `MERGE INTO` on a shared volume. Sail writes; **delta-rs** reads `_delta_log` and the rows. A Sail `COUNT(*)` after DML is not a witness. Standalone `UPDATE` is forwarded and Sail answers FAILED (`CommandNode::Update`) — never a silent no-op. Two concurrent `INSERT OVERWRITE`s: each success has its own log version; rows are one overwrite, not a silent merge. Photon is not this row. | 🟢 Real |
| Delta writes — UC three-part names | SDK creates an EXTERNAL table in UC OSS (Docker sidecar). Sail's unity catalog provider (`SAIL_CATALOG__LIST`, same Compose network) resolves `cat.sch.tbl`. Warehouse `INSERT` writes; **delta-rs** confirms the log. `INSERT` is not rewritten into a path. `CREATE TABLE cat.sch.t` with no `LOCATION` is rewritten to an EXTERNAL path under `file:///data/delta/managed` (Sail writes; this process registers UC). hive_metastore is not rewritten. | 🟢 Real |
| Delta maintenance — OPTIMIZE / VACUUM | Warehouse SQL. Sail cannot plan these. The family's spark-agent runs them through **delta-rs** (named shim). ZORDER and WHERE are refused, not silently dropped. Photon is not this row. | 🟢 Real |
| Delta writes — JVM overlay | Warehouse SQL on Apache Spark 3.5.5 + delta-spark (`make e2e-delta-jvm`), not Sail. Same **delta-rs** confirmer. `OPTIMIZE … ZORDER` is this row. | 🟠 Non-default engine |
| Clusters as session handle | `POST /api/2.0/clusters/create` starts a Sail session (`print(1)` via the HTTP agent) or fails naming the missing engine. Never sleeps to `RUNNING`. Autoscale and cluster libraries stay refused. | 🟢 Real |
| Cluster Policies / Policy Families / compliance | Policies persist. Create is denied when it violates `fixed` / `range` / `forbidden` / `allowlist` on `spark_version`, `node_type_id`, `num_workers`, `autoscale`, `libraries`. Unknown attributes are 501, not stored-and-ignored. One policy family: `emulator-session`. `get-compliance` reports the stored handle against that policy. | 🟢 Real |
| Command Execution | `/api/1.2/contexts` + `/commands` on a RUNNING cluster handle. Python and SQL run on the attached Sail agent (`kind: python` / `kind: sql`). Scala / R are 501. Without the engine, context create fails naming `DATABRICKS_SPARK_CONNECT_URL`. | 🟢 Real |
| Databricks Connect | After PAT/OIDC and `x-databricks-cluster-id` naming a RUNNING handle, `application/grpc` / `/spark.connect.…` is reverse-proxied to `DATABRICKS_SPARK_CONNECT_GRPC_URL` (Sail `:50051`, h2c). The HTTP agent is not this backend; only that URL set is 501 naming the gRPC variable. Authorization stripped before the engine. | 🟢 Real |
| Clusters as VMs | No hypervisor. A session handle is not a VM. | 🔴 Not implemented |
| Photon / DBR compatibility | No Photon attach exists. Sail is Spark SQL over Spark Connect. | 🔴 Not implemented |

## Catalog

| Feature | Emulator | Type |
|---|---|---|
| Unity Catalog CRUD | Reverse-proxy to UC OSS (`DATABRICKS_UC_URL`) after PAT/OIDC. Without a sidecar those routes are 501 naming the missing URL. MANAGED table create is refused (UC OSS only creates EXTERNAL tables at a filesystem location). Three-part SQL against those tables is the Delta writes row, not this one. | 🟢 Real |
| Unity Catalog grants | Enforcement, not allow-all CRUD. Not shipped until they deny. | 🔴 Not implemented |

## Tracking

| Feature | Emulator | Type |
|---|---|---|
| MLflow Experiments / Model Registry | File-backed tracking store under `data/mlflow/`. Experiments, runs (params / metrics / tags), registered models and versions persist across restart. Duplicate experiment names are 409. Artifact list / `log-model` / traces / logged-models are 501 — metadata only, not a model binary store. Model Serving is a different row. | 🟢 Real |

## Clients

| Feature | Emulator | Type |
|---|---|---|
| MCP — Databricks SQL | `POST /api/2.0/mcp/sql` JSON-RPC after PAT/OIDC. `execute_sql` / `poll_response` wrap the warehouse statements handler (same Sail attach, same `dialect: spark-sql`). Genie / AI Search / UC function MCP paths stay 501. | 🟢 Real |
| Terraform / DAB pair | Unmodified `databricks/databricks` and Databricks CLI `v1.12.1`: current_user + notebook + workspace_file + job create. `token=dev` refused. `bundle deploy` is not this row — current DAB schema also demands a cluster, and Permissions stay 501. Job *execution* is the engine row, not this one. | 🟢 Real |
| databricks-target toggle | Published `databricks-target` package. `DATABRICKS_TARGET=emulator\|real` resolves host, token, warehouse-by-name, catalog, vault. Consumer code holds names. `make e2e-databricks-target` creates `contoso_warehouse`, resolves it, `SELECT 1`. Real mode refuses localhost and seed secrets. | 🟢 Real |
| dbt-databricks warehouse run | Unmodified `dbt-databricks==1.12.4` `dbt run` of `one` (`select 1 as id`) and `two` (`select id from {{ ref('one') }}`) over HiveServer2. The gold gate (`make e2e-dbt-uc`) sets `catalog` (not `hive_metastore`), `+file_format: delta`, no post-hook; the warehouse shim writes `file:///data/delta/managed/…` and **delta-rs** plus a three-part `SELECT` confirm the rows. `make e2e-dbt` remains hive_metastore Thrift smoke (volume copy). Jobs `dbt_task` is a separate client path with its own gate, `ci:e2e-dbt-task`. | 🟢 Real |

## Published APIs outside this slice

One row per remaining group on the [workspace REST API reference](https://docs.databricks.com/api/workspace/)
sidebar. The emulator column is what an honest attach would have to be —
same style as the greens — not a product-page promise.

| Feature | Emulator | Type |
|---|---|---|
| Global Init Scripts | Applied to a real cluster VM — we have no VMs. | 🔴 Not implemented |
| Instance Pools / Instance Profiles | Real VMs / cloud instance profiles. | 🔴 Not implemented |
| Managed Libraries | Installed on a real cluster VM. JARs on Sail have no classloader (fabric's JVM overlay is the 🟠 path; this repo does not ship one). | 🔴 Not implemented |
| Apps | Would need a deployed app process. | 🔴 Not implemented |
| SCIM Groups / Users / Service Principals | Directory mutations that then deny. | 🔴 Not implemented |
| Permissions | Enforcement, not allow-all. | 🔴 Not implemented |
| SQL Alerts | Would evaluate a stored query on a schedule. No alert evaluator is attached. | 🔴 Not implemented |
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
