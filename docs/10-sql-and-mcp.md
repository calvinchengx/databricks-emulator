# 10 — SQL warehouses and MCP

A SQL warehouse is a session handle onto the same Spark agent Jobs use — not
a VM and not Photon. The response names `dialect: spark-sql` so a client that
assumed Photon can tell.

## Warehouses

`POST /api/2.0/sql/warehouses` with a `name` creates a **RUNNING** handle
(`id` like `wh-1`). List, get, start, stop, delete are implemented. Stop, then
execute: the statement is `FAILED` (`warehouse is STOPPED`).

Without `DATABRICKS_SPARK_CONNECT_URL`, execute is `FAILED` naming the missing
engine.

## Statements

`POST /api/2.0/sql/statements` with `warehouse_id` and `statement`. The
emulator sends the SQL as `kind: sql` — the `code` **is** Spark SQL, not
`print(spark.sql(...))`. The family's spark-agent dispatches on `kind`.

```python
wh = w.warehouses.create(name="e2e").result()
stmt = w.statement_execution.execute_statement(
    warehouse_id=wh.id, statement="SELECT 1",
)
print(stmt.status.state)  # SUCCEEDED
```

The official SDK drops unknown fields. `dialect: spark-sql` lives on the wire
(`GET /api/2.0/sql/statements/{id}`). `executedBy` says Spark SQL, not Photon.

`sql_task.file` jobs take this same path. `sql_task.query`, dashboard, and
alert are refused at job create.

Witness: `ci:e2e-engine`.

## Thrift / HiveServer2

The warehouse connector does not call the statements API. Unmodified
`databricks-sql-connector==4.4.0` POSTs `application/x-thrift`
(`TBinaryProtocol`) to `/sql/1.0/endpoints/{warehouse_id}` after PAT.
`/sql/protocolv1/o/{org}/{id}` is the same processor when `{id}` is a
known warehouse. OpenSession binds the session to that id; ExecuteStatement
reuses `runSQLStatement` (same Sail `kind: sql` attach). Tiny results are
inline `COLUMN_BASED_SET`. Cloud Fetch, Arrow+LZ4, and metadata RPCs are
refused by name. Missing engine fails naming `DATABRICKS_SPARK_CONNECT_URL`.

Witness: `ci:e2e-sql`.

## Delta writes

`CREATE TABLE … USING delta LOCATION`, `INSERT`, `DELETE`, and `MERGE INTO`
go down this same warehouse path onto Sail. The witness is **not** a Sail
`COUNT(*)`. `make e2e-delta` writes to a shared volume and **delta-rs**
(`deltalake`) reads `_delta_log` and the rows. The engine that wrote is
never the one that confirms. Standalone `UPDATE` is forwarded; Sail
answers FAILED (`CommandNode::Update`), never a silent no-op.

Three-part `INSERT INTO cat.sch.tbl` uses the same warehouse path. UC OSS
and Sail share the e2e Compose network: this process proxies catalog REST
(`DATABRICKS_UC_URL`); Sail's unity catalog provider
(`SAIL_CATALOG__LIST`, `UNITY_ALLOW_HTTP_URL`) resolves the name. The
emulator does not rewrite `cat.sch.tbl` into a path. An EXTERNAL table
with no `_delta_log` at `storage_location` is not yet a Delta table —
the LOCATION write in the same job creates that log, then the three-part
INSERT is the named-table witness.

`OPTIMIZE` and `VACUUM` take the same warehouse path. Sail has no grammar
for them (`found OPTIMIZE at 0:8`). The family's spark-agent routes those
statements through **delta-rs** — a named shim, the same one fabric uses.
The files change; Spark does not run a job. The e2e witness addresses the
table as `OPTIMIZE delta.\`uri\`` (self-describing). `OPTIMIZE … ZORDER` /
`WHERE` are refused rather than silently ignored.

Two concurrent warehouse `INSERT OVERWRITE`s: each success advances its own
log version; the rows are one overwrite, not a silent merge.

`OPTIMIZE … ZORDER` is the JVM overlay (`make e2e-delta-jvm`): Apache Spark
3.5.5 + delta-spark, same delta-rs confirmer. That row is 🟠.

Photon is not this path. See [parity.md](parity.md).

Witness: `ci:e2e-delta`.

## MCP — Databricks SQL

`POST /api/2.0/mcp/sql` is JSON-RPC after PAT/OIDC. `initialize` returns a
`Mcp-Session-Id`. Tools:

| Tool | What it wraps |
|---|---|
| `execute_sql` | the warehouse statements handler (`query`, optional `_meta.warehouse_id`) |
| `poll_response` | `GET` of that statement |

If `_meta.warehouse_id` is omitted, the first RUNNING warehouse is used. None
running → tool error. The tool result is the same statement JSON, so
`SUCCEEDED` and `spark-sql` are in the payload.

`GET` is 405. `DELETE` ends the session (204). Every other `/api/2.0/mcp/*`
(Genie, AI Search, UC functions) is 501 naming this SQL surface.

Witness: `ci:e2e-engine`.
