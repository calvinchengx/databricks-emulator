# Doctrine

A Databricks workspace emulator is possible on the same bet the Azure emulator
family already uses: terminate the public REST, attach a real engine, refuse
what you cannot compute.

This document is the founding constraint. Implementation that contradicts it
is a bug, not a shortcut.

## What this is

A sibling of [fabric-emulator](https://github.com/calvinchengx/fabric-emulator).
Its own host (`adb-*.azuredatabricks.net` wire shape), own OAuth/PAT, own
Workspace and Jobs surfaces. Fabric *consumes* Databricks; this process *is*
the workspace.

Do not fold the workspace API into fabric-emulator. That would blur the trust
boundary the way inlining Key Vault would have.

## What this is not

- A Databricks Runtime replacement. "DBR 15.4 LTS compatible" is a claim this
  repo will not make.
- An extension of Fabric's Databricks *activities*. Those already terminate
  locally and refuse `dbfs:` / `/Workspace` / `/Repos` paths by name unless
  `FABRIC_DATABRICKS_URL` points here. This repo is the host those paths
  resolve against.
- A lookalike. A cluster create that does not run Spark, a SQL warehouse that
  answers Photon with DuckDB and does not say so, or a Permissions API that
  stores grants and always allows, are refused — not shipped as green.

## Identity

Identity is **PAT + Databricks OIDC**. Entra is an optional federated issuer
(`DATABRICKS_OIDC_ISSUERS`). The binary `make run`s with no entra-emulator.
Any-token-accepted is not identity: `"dev"` is 401 unless that exact value was
minted as a PAT (the seeder will not).

## First honest slice

Enough that `databricks-sdk` and fabric-emulator's Databricks activities can
point at the same host:

1. Jobs API 2.2 (2.1 aliased) — Python / notebook on an attached Spark engine.
2. Workspace files / notebooks — file-backed store, including workspace-files.
3. DBFS / Files API — real bytes on a local blob root.
4. PAT and emulator OIDC. Federated JWT when an issuer is configured.

Unity Catalog CRUD attaches [UC OSS](https://github.com/unitycatalog/unitycatalog)
(`DATABRICKS_UC_URL`); there is no invented metastore. Grants stay refused
until they deny. SQL warehouses and Databricks Connect attach the same Spark
engine. Clusters as VMs and Photon never.

## Reachable, if honest

| Feature | Honest attach |
|---|---|
| Workspace files / notebooks | File-backed store |
| Jobs 2.2 | Python / notebook on Sail or JVM Spark |
| DBFS / Files API | Local blob under `data/dbfs/` |
| Secrets | Databricks-backed persist; AKV-backed is a live vault read-through, not a sync |
| SQL warehouses | Spark SQL, dialect named in the output |
| Clusters | Session handle onto the attached engine, not a VM |
| Unity Catalog CRUD | UC OSS sidecar |
| Databricks Connect | Spark Connect |
| Identity | PAT + emulator OIDC; Entra optional |

## Permanently red

- Photon and any "DBR compatible" claim
- Full `dbutils` / `spark.databricks.*` runtime
- Cluster autoscaling, instance pools, serverless SQL as real VMs
- Lakeflow / DLT
- Model Serving, Vector Search, Dashboards
- JAR main on a Python-only agent
- Fine-grained Unity Catalog grants, if the Permissions API does not enforce them

## Proof

The **catalog** of what real Databricks offers at the workspace host is the
[workspace REST API reference](https://docs.databricks.com/api/workspace/).
Account-level APIs and Databricks Runtime are out of that catalog.

A row is green only when a witness exists: an unmodified client (`databricks-sdk`,
`databricks/databricks` Terraform, Databricks CLI, or fabric-emulator's Databricks activity) drove the call, and
the engine or store actually did the work. Status without a witness is not
support. See [parity.md](parity.md), then the [quickstart](01-quickstart.md).
