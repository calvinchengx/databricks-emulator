# Feature parity: databricks-emulator vs. a real Databricks workspace

How the emulator's surface maps to Databricks' public REST (Workspace, Jobs,
Unity Catalog), and — the point of this table — **whether real work happens or
just the API shape**.

A row is 🟢 **Real** only when an unmodified client drove the call and the
attached engine or store actually did the work. Status without a witness is
not support. See [00-doctrine.md](00-doctrine.md).

**Witnessed claims: 0.** The repository is founded. Nothing is green.

| Surface | What would make it real | Status |
|---|---|---|
| Jobs 2.1 — notebook / Python | Attached Spark engine executes the file | ⬜ not started |
| Jobs 2.1 — JAR main | A submission path that loads a Java/Scala main class | 🔴 refuse (Python-only agent) |
| Workspace files / notebooks | File-backed store, `databricks-sdk` / CLI round-trip | ⬜ not started |
| DBFS / Files API | Real bytes on a blob store | ⬜ not started |
| Identity (PAT / OAuth) | entra-emulator tokens; unauthenticated callers fail | ⬜ not started |
| Secrets | Real store; values only in job env | ⬜ not started |
| SQL warehouses | Spark SQL, dialect named in the output | ⬜ not started |
| Clusters as VMs | — | 🔴 refuse (session handle onto the engine, not a VM) |
| Unity Catalog CRUD | UC OSS sidecar | ⬜ not started |
| Unity Catalog grants | Enforcement, not allow-all CRUD | ⬜ not started |
| Databricks Connect | Spark Connect | ⬜ not started |
| Photon / DBR compatibility | — | 🔴 refuse |
| Lakeflow / DLT | — | 🔴 refuse |
| Model Serving / Vector Search / Dashboards | — | 🔴 refuse |
