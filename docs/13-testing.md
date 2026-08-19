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
| `make e2e-cli` | Unmodified Databricks CLI `v1.12.1` (`e2e/cli/run.py`, CI job `e2e-cli`). |
| `make e2e-terraform` | Unmodified `databricks/databricks` provider (`e2e/terraform/run.py`, CI job `e2e-terraform`). |
| `make e2e-engine` | Unmodified `databricks-sdk==0.129.0` and `databricks-connect==19.1` with Sail + spark-agent (`e2e/engine/run.py`, CI job `e2e-engine`). Needs Docker and uv (Python 3.12 via `.python-version`). |
| `make e2e-delta` | Warehouse SQL writes Delta via Sail; **delta-rs** confirms the log (`e2e/delta/run.py`, CI job `e2e-delta`). Needs Docker. |
| `make e2e-delta-jvm` | Warehouse SQL writes Delta via JVM Spark + delta-spark; **delta-rs** confirms (`e2e/delta-jvm/run.py`, CI job `e2e-delta-jvm`). 🟠 overlay. Needs Docker. |
| `make e2e-uc` | Unmodified `databricks-sdk==0.129.0` with UC OSS (`e2e/uc/run.py`, CI job `e2e-uc`). Needs Docker. |
| `make e2e-sql` | Unmodified `databricks-sql-connector==4.4.0` over HiveServer2 Thrift (`e2e/sql/run.py`, CI job `e2e-sql`). Needs Docker. |
| `make e2e-databricks-target` | Published `databricks-target` resolver (`e2e/databricks-target/run.py`, CI job `e2e-databricks-target`). Warehouse by name + `SELECT 1`. Needs Docker. |
| `make e2e-dbt` | Unmodified `dbt-databricks==1.12.4` hive_metastore Thrift smoke (`e2e/dbt/run.py`, CI job `e2e-dbt`). Needs Docker. |
| `make e2e-dbt-uc` | Unmodified `dbt-databricks==1.12.4` against a Unity Catalog catalog (`e2e/dbt-uc/run.py`, CI job `e2e-dbt-uc`). Gold-shaped: catalog set, no post-hook. Needs Docker. |

Pin the SDK in the root `pyproject.toml` / `uv.lock` (`sdk`, `engine`,
`delta`, `sql`, `target`, `dbt` groups). A floating `pip install databricks-sdk` is not a witness —
it is whatever PyPI shipped the morning CI ran. Same toolchain as
fabric-emulator: `uv run --frozen --group <name>`. The `dbt` group
conflicts with the others (`dbt-databricks==1.12.4` needs
`databricks-sdk<0.118`). The 85% `go test` floor
excludes `internal/hs2/cliservice` (generated Spark-fork TCLIService).

## Witness kinds

| kind | what it means |
|---|---|
| `ci:<job>` | a CI job driving a real external client |
| `go:<Test>` | a Go test: real HTTP, real store, this repo's client on both ends |

Parity rows are fabric-style: feature, what the shim terminates, which engine
computes, then 🟢 / 🟡 / 🟠 / 🔴. Only 🟢 needs a witness.

`ci:` is stronger. The family evidence table counts each green claim once, by
its strongest witness. Do not file a `ci:` on a claim the stranger did not
drive.

## What each CI job actually drives

**`e2e-sdk`** — PAT + `token=dev` 401; emulator OIDC M2M `Me`; federated JWT
doors (unconfigured / wrong aud / expired / garbage → 401, good JWT → `Me`);
workspace AUTO upload/download; DBFS put/read; git-credentials + repos
clone/commit/pull against a local git remote; cluster policy mismatch is
400 (unknown attributes 501); MLflow experiment + run metrics + registered
model version persist across restart (artifact list is 501); secrets persist
across restart; cluster-create **without** an engine must fail naming
`DATABRICKS_SPARK_CONNECT_URL`.

**`e2e-cli`** — pinned Databricks CLI `v1.12.1` (not PATH). `current-user me`
/ `token=dev`; workspace SOURCE notebook + RAW file import/export; `fs`
mkdir/cp/cat/ls; secrets persist across restart with the value **withheld**;
`tokens create`; warehouse create/list; `clusters spark-versions`; job
**create** (not execution). That is the CLI half of the DAB pair.
`bundle deploy` is not this job.

**`e2e-terraform`** — PAT / `token=dev`; notebook; workspace file; job
**create** (not execution). That is the DAB pair.

**`e2e-engine`** — cluster session `RUNNING`; `databricks-connect`
`SELECT 1`; Command Execution `print('CMD-REACHED')` on that handle;
Python job logs contain `REACHED`; `{{secrets}}` printed from
`os.environ`; AKV rotate visible on the next run; warehouse `SELECT 1`
names `dialect: spark-sql`; stored query create/get/list then execute
that `query_text`; query history lists the `FINISHED` run; alerts are
501; MCP `execute_sql`.

**`e2e-sql`** — pinned `databricks-sql-connector==4.4.0` (not PATH, no
pyarrow extra). Warehouse create via REST; `sql.connect` to
`/sql/1.0/endpoints/{id}`; `SELECT 1` fetches one typed cell. `token=dev`
is refused. That is HiveServer2, not `POST /api/2.0/sql/statements`.

**`e2e-dbt`** — pinned `dbt-databricks==1.12.4`. hive_metastore Thrift
smoke: `dbt debug` + `dbt run` of `one` / `two` over HiveServer2, then
**delta-rs** reads a post-hook copy on a mounted volume. `token=dev` is
refused. Jobs `dbt_task` is not this job -- that is `e2e-dbt-task`, where
the runner has no dbt at all and dbt runs on the agent.

**`e2e-condition-task`** — if/else, driven by the unmodified `databricks-sdk`
(`jobs.ConditionTask`, `jobs.TaskDependency(outcome=...)`) and read back by
**delta-rs**, so which arm ran is a Delta table on disk rather than the run
record the emulator wrote about itself. Both operator families run on the SAME
operands — `12.0 == 12` false as strings, `12.0 >= 12` true as numbers — because
either check alone passes under a single-family implementation. Both arms write,
to different tables, so a branch that should have been SKIPPED is caught by its
table appearing rather than assumed absent.

**`e2e-dbt-task`** — dbt as a JOB. Nothing on the runner has dbt: the suite
runs under the `delta` group and asserts `import dbt` fails before it starts,
so a pass cannot mean dbt ran locally. The project is imported into the
workspace as RAW files, a `dbt_task` names it and a warehouse, and the emulator
carries the project inline to the agent, where dbt runs against that warehouse
over `host.docker.internal` — the agent dialling back out, which is what a
dbt_task does on real Databricks. **delta-rs** confirms the rows from a
post-hook copy, and `task_two` selects from `task_one` so a resolved-but-unrun
DAG is a missing table rather than a green run.

**`e2e-dbt-uc`** — the gold-shaped gate. Same adapter pin; `catalog: e2e`,
`schema: gold`, `+file_format: delta`, no post-hook. UC OSS + Sail's
unity catalog provider share the Compose network. The warehouse shim
writes `managed/e2e/gold/{one,two}`; **delta-rs** confirms those
directories, then `SELECT id FROM e2e.gold.two` through the warehouse.
`token=dev` is refused.

**`e2e-delta`** — warehouse `CREATE TABLE … USING delta LOCATION` + `INSERT`
+ `DELETE` + `MERGE INTO` through unmodified `databricks-sdk`. Confirmation
is delta-rs on the shared volume (`_delta_log` exists, rows match, version
advances). Standalone `UPDATE` is FAILED (`CommandNode::Update`). Sail
`COUNT(*)` is not the witness. A UC EXTERNAL table then names that
`storage_location`; three-part `INSERT INTO e2e.s.events` writes through
Sail's unity catalog provider (same Compose network as UC OSS) and
delta-rs confirms the new row. `CREATE TABLE e2e.s.from_shim … AS SELECT`
with no `LOCATION` is rewritten to an EXTERNAL path; delta-rs confirms
that directory, then a three-part INSERT. `OPTIMIZE` / `VACUUM` use
`delta.\`file://…\`` through the spark-agent's delta-rs shim (Sail has no
grammar); `OPTIMIZE … ZORDER` is refused. Two concurrent `INSERT OVERWRITE`s
on a second table: each success has its own log version; rows are one
overwrite, not a silent merge.

**`e2e-delta-jvm`** — the 🟠 overlay. Warehouse SQL on Apache Spark 3.5.5 +
delta-spark (no Sail). Confirmation is still delta-rs. `OPTIMIZE … ZORDER`
is this job.

**`e2e-uc`** — `catalogs.create` / `schemas.create` / EXTERNAL `tables.create`
plus `tables.get`; MANAGED create and `grants.get` are 501. The sidecar is
`unitycatalog/unitycatalog:v0.5.0`. Missing sidecar is the Go test, not this
job.

**`e2e-databricks-target`** — the published consumer toggle
(`python/databricks-target`). Unit suite plus a live emulator: resolve
`DATABRICKS_TARGET=emulator`, create warehouse `contoso_warehouse`,
resolve it by name, `SELECT 1`. Real-target conformance is
secret-gated, not this job.

## Not a witness

- A Go test that reports SUCCESS from a scripted hook without the production
  agent. Those tests prove routing and refusal; they do not replace `ci:e2e-engine`.
- `curl` in a README.
- A doc page that names an endpoint.

The family chain test in azure-emulators is a **seam** check on published
images, not this repo's Jobs witness. See
[Family integration](14-family-integration.md).
