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
