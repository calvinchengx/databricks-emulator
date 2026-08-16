# 08 — Jobs and the Spark attach

Jobs 2.2 (2.1 aliased) run a Python file or notebook on an attached statement
agent. Without `DATABRICKS_SPARK_CONNECT_URL`, `run-now` fails naming the
missing engine — never `SUCCESS`. That refusal is the feature.

## Attach Sail

This repo's engine e2e is the supported attach: Sail (Rust Spark Connect) plus
the family's published spark-agent.

```bash
make e2e-engine
```

That script `docker compose`s [`e2e/engine/docker-compose.yml`](../e2e/engine/docker-compose.yml),
builds this binary, sets `DATABRICKS_SPARK_CONNECT_URL=http://127.0.0.1:8099`
(HTTP agent) and `DATABRICKS_SPARK_CONNECT_GRPC_URL=http://127.0.0.1:50051`
(Sail), and drives unmodified `databricks-sdk` plus pinned
`databricks-connect==19.1`. `ENTRA_TOKEN_URL` is
deliberately unset on Sail so the launcher execs the server instead of
waiting on a Storage-audience mint. `uv run --frozen --group engine`
supplies Python 3.12 (`databricks-connect==19.1` Requires-Python).

To attach by hand:

```bash
docker compose -f e2e/engine/docker-compose.yml -p dbx-engine up -d --wait
DATABRICKS_DISABLE_TLS=1 \
  DATABRICKS_SPARK_CONNECT_URL=http://127.0.0.1:8099 \
  DATABRICKS_SPARK_CONNECT_GRPC_URL=http://127.0.0.1:50051 \
  make run
```

The agent listens on `:8099` (`POST /statements`, `GET /health`). Sail is
`:50051`. Images are `ghcr.io/calvinchengx/emulator-sail:0.7.0` and
`…/emulator-spark-agent:4.2.0`, both pinned by digest in every compose file.

Each tag names the **dependency the image is pinned for** — the Sail engine
version, and the pyspark-client version — rather than the release number of the
repo that publishes them. That is what a consumer actually pins these for.

**The digests are the pin; the tags are documentation.** Neither tag moves when
fabric-emulator releases, so every release rebuilds and overwrites both. v0.27.0
did exactly that: `emulator-sail:0.7.0` went `807adeb9…` → `0d2fe3c7…` while
remaining Sail 0.7.0. Reading a tag alone would silently change the engine under
a stack that had been witnessed against a different build, so the digests here
are refreshed together, from one release, rather than drifting apart.

Family compose in azure-emulators does **not** set this URL. A job created
from a Fabric Databricks activity against that stack fails naming the missing
engine — an honest pass for the chain test, not a green Jobs row.

## What the agent must do

`POST {url}/statements` with `{session, code, kind, env, spark_conf}`.

- `kind: python` — run the code. Cluster create sends `print(1)`. A Python
  job sends the workspace/DBFS file, with an argv or notebook-param preamble.
- `kind: sql` — the `code` **is** Spark SQL, not `print(spark.sql(...))`.
  Warehouses and `sql_task.file` take this path. The statement response names
  `dialect: spark-sql`.

A toy agent that reports `SUCCESS` without Spark is a lookalike and is
refused. See [Doctrine](00-doctrine.md).

## What `e2e-engine` proves

Unmodified `databricks-sdk` 0.129 and `databricks-connect` 19.1:

1. `clusters.create` waits until `RUNNING` (session handle, not a VM).
2. `DatabricksSession.builder.remote("sc://localhost:18449/;…")` then
   `spark.sql("SELECT 1 AS n").collect()` is `[Row(n=1)]`.
3. Upload `/Shared/reached.py`, `jobs.create` + `run_now_and_wait`;
   `get_run_output` logs contain `REACHED`.
4. Missing `{{secrets/kv/nope}}` fails the run.
5. Warehouse `SELECT 1` succeeds and the wire names `dialect: spark-sql`.
6. MCP `execute_sql` takes the same warehouse handler.

Witness: `ci:e2e-engine` on clusters-as-session, Databricks Connect, Jobs
Python, SQL warehouses, and MCP SQL.

## What it does not prove

| Claim | Why not |
|---|---|
| JAR / dbt / DLT / `sql_task.query` | Refused at job create. |

Create a Python job without an engine to see the refusal:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.jobs import SparkPythonTask, Task
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient(host="http://127.0.0.1:8447", token=open("data/admin.pat").read().strip())
w.workspace.upload("/Shared/hi.py", b"print('hi')\n", overwrite=True, format=ImportFormat.AUTO)
job = w.jobs.create(name="hi", tasks=[Task(task_key="t", spark_python_task=SparkPythonTask(python_file="/Shared/hi.py"))])
try:
    w.jobs.run_now_and_wait(job_id=job.job_id)
except DatabricksError as exc:
    print(exc)  # names DATABRICKS_SPARK_CONNECT_URL
```
