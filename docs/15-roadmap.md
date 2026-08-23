# 15 — Roadmap

Same discipline as the family: each slice independently useful, witnessed by
an unmodified client, refuse what you cannot compute. The catalog is the
[workspace REST API reference](https://docs.databricks.com/api/workspace/).
Account-level APIs and Databricks Runtime stay out.

## Done — first honest slice

Identity (PAT, emulator OIDC, federated JWT), workspace SOURCE/PYTHON and raw
files, DBFS, Jobs 2.2 Python/notebook on an attached engine, Databricks-backed
secret persist, SQL warehouses / MCP SQL / HiveServer2 Thrift / clusters-as-session on that engine,
Terraform/DAB pair. Green rows and their witnesses: [parity.md](parity.md).

Independently evidenced (`ci:`) as of this writing: identity, workspace, DBFS,
Git Credentials / Repos (git clone into the workspace store), cluster
policies (enforced on create), Command Execution (context on Sail),
MLflow experiments / model registry (file-backed tracking store), secret persist
and injection, AKV read-through + vault-audience, Terraform/DAB,
clusters-as-session, Databricks Connect, Jobs Python, SQL warehouses, SQL Queries / Query History, HiveServer2 Thrift (`databricks-sql-connector==4.4.0`), dbt-databricks warehouse run (UC catalog + hive_metastore), MCP SQL,
Unity Catalog CRUD, the `databricks-target` emulator/real toggle,
Delta writes (Sail write, delta-rs confirm: INSERT,
DELETE, MERGE; UPDATE fails loudly; three-part `INSERT INTO cat.sch.tbl`
via Sail's unity catalog provider; `OPTIMIZE`/`VACUUM` via the spark-agent
delta-rs shim, ZORDER refused; concurrent `INSERT OVERWRITE` serialises).

## dbt_task

Gold can be built THROUGH a job now, not from a host script beside one. The
project travels from the workspace store to the agent and dbt runs against the
warehouse the task names, which is what real Databricks does: dbt is a
warehouse client either way, and the job only changes who invokes it.

It needs `dbt-databricks` on the statement agent. An agent without it fails
saying so rather than reporting a run that built nothing.

`target/run_results.json` comes back on the run output as
`dbt_output.artifacts`, and it comes back on a FAILING run too -- the agent
emits it before re-raising, because a failing `dbt test` is exactly when a
caller needs to know which test failed rather than only that one did. Without
that, a caller whose snapshot lists contract failures would have had to give
the list up to move onto Jobs, trading the right execution shape for the
evidence the execution exists to produce. Shipped in v0.2.7; the task itself
in v0.2.6.

**This does not by itself close G4** in `contoso-data-product`'s plan, and an
earlier version of this section said it did. Two things stand between the
capability and that cell being green, and neither is ours:

  * the consumer still runs gold from a host script. A capability nothing
    invokes changes no cell; its step has to be rewritten as a `dbt_task`.
  * even then, DoD 3 asks for the pipeline to run through the orchestrator the
    cell is named for, and gold is one step of seven. The other six --
    provision, ingest, bronze, silver, register, govern -- also run from the
    host today.

What this emulator owed that cell, it has now paid: the task type, and the
artefact that makes it usable.

## Orchestration the shim can compute

`condition_task` is in: if/else branching decided in this process, with
`depends_on.outcome` selecting the arm, and no engine involved. It needs none,
which is what makes it honest to implement here rather than refuse.

`for_each_task` and `run_job_task` are the rest of that family and are the
obvious next two, for the same reason: both are pure orchestration this
process can compute in full. They are refused **by name** today so the gap is
enumerated in [parity.md](parity.md) rather than hidden behind a generic
"unknown task type".

## Sidecars attached

The first-slice greens that needed a sidecar now have `ci:`. Grants stay 501
until they deny, that is 🔴 Not implemented, not a leftover green. What is
actually next, graded, is [Not implemented](#not-implemented) below, in
parity.md order rather than guessed here.

Do not invent a fake statement agent, metastore, or Permissions allow-all to
close red rows. That rule outlives any one slice, so it stays here rather
than moving with the work it was written for.

## Docs still to write

No local `make up` / `make status`, and none planned: this repo's quickstart
is native (`make run`, see [01](01-quickstart.md)); the family stack comes
from `azure-emulators`'s `docker compose --profile databricks up` ([14](14-family-integration.md)).
A platform-setup chapter here would document a compose file this repo does
not ship.

Parity history is generated from every `v*` tag that carries `docs/parity.md`
(`v0.1.0` is the first). Live map, snapshots, and changelog live on the docs
site — not a numbered chapter.

## Not implemented

Photon, DBR version strings, full `dbutils`, cluster VMs, Lakeflow / DLT,
Model Serving, Vector Search, Dashboards, JAR main on a Python-only agent,
and Unity Catalog grants until they deny. The rest of the workspace REST
catalog is the same grade in [parity.md](parity.md) until a witness exists.
501, never a silent 200.
