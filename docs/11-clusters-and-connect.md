# 11 — Clusters and Databricks Connect

A cluster here is a **session handle** onto the attached Spark engine, not a
VM. Create runs `print(1)` on the agent; success → `RUNNING` with
`state_message` "session handle onto the emulator's Spark engine, not a VM".
It never sleeps to `RUNNING`.

## Create, start, delete

```python
created = w.clusters.create(
    cluster_name="e2e",
    spark_version="emulator-spark",
    node_type_id="emulator.session",
    num_workers=0,
).result()
print(created.state)  # RUNNING
```

Empty `spark_version` / `node_type_id` default to `emulator-spark` and
`emulator.session`. `GET …/spark-versions` and `…/list-node-types` return
those two and say they are not a DBR and not a VM.

Without `DATABRICKS_SPARK_CONNECT_URL`, create is 400 naming the missing
engine (`INVALID_STATE`). Autoscale is 400: "clusters are a session handle,
not a VM pool". `libraries` is 400: this process does not own a cluster
lifecycle to install onto.

Start re-runs the session probe. Delete / permanent-delete drop the handle.

Witness: `ci:e2e-engine`. `e2e-sdk` proves the no-engine refusal.

## Cluster policies

Policies persist under `data/policies/`. Every attribute in the definition
is one this process actually checks on `clusters/create`: `spark_version`,
`node_type_id`, `num_workers`, `autoscale`, `libraries`. Types: `fixed`,
`range`, `forbidden`, `allowlist`, `unlimited`. Anything else is 501 — a
policy that would not be enforced is not stored.

One policy family is listed: `emulator-session` (session handle, not a VM).
`GET /api/2.0/policies/clusters/get-compliance` reports the stored handle
against its policy.

Witness: `ci:e2e-sdk` (mismatch is 400 naming the field; matching policy
still fails naming the missing engine; unknown attributes 501).

## Command Execution

`/api/1.2/contexts/create` and `/api/1.2/commands/execute` after PAT/OIDC.
The cluster must be RUNNING. Python and SQL are forwarded to the same
statement agent Jobs and warehouses use (`kind: python` / `kind: sql`).
Scala and R are 501. Without `DATABRICKS_SPARK_CONNECT_URL`, context
create fails naming the engine.

```python
from databricks.sdk.service.compute import Language
ctx = w.command_execution.create(cluster_id=cluster_id, language=Language.PYTHON).result()
cmd = w.command_execution.execute(
    cluster_id=cluster_id,
    context_id=ctx.id,
    language=Language.PYTHON,
    command="print(1)",
).result()
print(cmd.results.data)
```

Witness: `ci:e2e-engine` (unmodified SDK `print('CMD-REACHED')` on Sail).

## Databricks Connect

After PAT/OIDC, a gRPC request (`Content-Type: application/grpc` or path
`/spark.connect.…`) is reverse-proxied to
`DATABRICKS_SPARK_CONNECT_GRPC_URL` (Sail `:50051`). The HTTP statement
agent at `DATABRICKS_SPARK_CONNECT_URL` is not this backend.

Required:

- `x-databricks-cluster-id` naming a **RUNNING** handle
- `DATABRICKS_SPARK_CONNECT_GRPC_URL`

The `Authorization` header is stripped before the backend sees it. Missing
cluster id is 400; unknown cluster is 404; not RUNNING is 400; no gRPC URL
is 501 even when the HTTP agent is set.

When TLS is off, the same port accepts REST HTTP/1 and Connect h2c (prior
knowledge). The outbound hop to Sail is h2c-only.

Witness: `ci:e2e-engine` — unmodified `databricks-connect==19.1` runs
`spark.sql("SELECT 1 AS n").collect()` through this proxy. The connection
string uses host `localhost` (not `127.0.0.1`): with a token, the client's
ChannelBuilder only skips TLS for that name.

See [Jobs and the Spark attach](08-jobs-and-spark.md) for the HTTP attach
`make e2e-engine` uses.
