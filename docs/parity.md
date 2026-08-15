# Feature parity: databricks-emulator vs. a real Databricks workspace

How the emulator's surface maps to Databricks' public REST (Workspace, Jobs,
Unity Catalog), and — the point of this table — **whether real work happens or
just the API shape**.

A row is 🟢 **Real** only when an unmodified client drove the call and the
attached engine or store actually did the work. Status without a witness is
not support. See [00-doctrine.md](00-doctrine.md).

**Witnessed claims: 10.** See [witnesses.json](witnesses.json).

| Surface | What would make it real | Status |
|---|---|---|
| Identity — PAT | Seeded/minted PAT; unknown / `dev` is 401 | 🟢 Real |
| Identity — emulator OIDC | Client-credentials → `Me` with no entra process | 🟢 Real |
| Identity — federated JWT | Opt-in issuer list; unconfigured / wrong aud / expired is 401 | 🟢 Real |
| Jobs 2.2 — notebook / Python | Attached Spark engine executes the file | 🟢 Real (engine attached) |
| Jobs 2.2 — JAR / dbt / DLT / sql_task | Refused at create | 🔴 refuse |
| Workspace files / notebooks | File-backed store, SDK / workspace-files round-trip | 🟢 Real |
| DBFS / Files API | Real bytes on a blob store | 🟢 Real |
| Secrets — Databricks-backed | Persist under `data/secrets/`; GET rejected; `{{secrets}}` in job env and `spark_conf`; missing fails the run | 🟢 Real |
| Secrets — Azure Key Vault-backed | Live read-through at use time; `put`/`delete` refused; rotate the vault secret and the next run sees it | 🟢 Real (vault attached) |
| SQL warehouses | Spark SQL, dialect named in the output | ⬜ not started |
| Clusters as VMs | — | 🔴 refuse (session handle onto the engine, not a VM) |
| Unity Catalog CRUD | UC OSS sidecar | ⬜ not started |
| Unity Catalog grants | Enforcement, not allow-all CRUD | ⬜ not started |
| Databricks Connect | Spark Connect | ⬜ not started |
| Photon / DBR compatibility | — | 🔴 refuse |
| Lakeflow / DLT | — | 🔴 refuse |
| Model Serving / Vector Search / Dashboards | — | 🔴 refuse |
