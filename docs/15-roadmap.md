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
clusters-as-session, Databricks Connect, Jobs Python, SQL warehouses, HiveServer2 Thrift (`databricks-sql-connector==4.4.0`), dbt-databricks warehouse run, MCP SQL,
Unity Catalog CRUD, Delta writes (Sail write, delta-rs confirm: INSERT,
DELETE, MERGE; UPDATE fails loudly; three-part `INSERT INTO cat.sch.tbl`
via Sail's unity catalog provider; `OPTIMIZE`/`VACUUM` via the spark-agent
delta-rs shim, ZORDER refused; concurrent `INSERT OVERWRITE` serialises).

## Next honest attaches

The first-slice greens that needed a sidecar now have `ci:`. Grants stay 501
until they deny — that is 🔴 Not implemented, not a leftover green.

Do not invent a fake statement agent, metastore, or Permissions allow-all to
close red rows.

## Docs still to write

P2 leftover: release notes on the next tag; platform-setup after `make up` /
`make status` exist.

Parity history is generated from every `v*` tag that carries `docs/parity.md`
(`v0.1.0` is the first). Live map, snapshots, and changelog live on the docs
site — not a numbered chapter.

## Not implemented

Photon, DBR version strings, full `dbutils`, cluster VMs, Lakeflow / DLT,
Model Serving, Vector Search, Dashboards, JAR main on a Python-only agent,
and Unity Catalog grants until they deny. The rest of the workspace REST
catalog is the same grade in [parity.md](parity.md) until a witness exists.
501, never a silent 200.
