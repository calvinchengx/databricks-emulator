# 12 — Unity Catalog

There is no invented metastore. `/api/2.1/unity-catalog/` and the `/api/2.0/`
alias reverse-proxy to a [UC OSS](https://github.com/unitycatalog/unitycatalog)
sidecar after PAT/OIDC. Without `DATABRICKS_UC_URL` those routes are 501
naming the missing sidecar.

Authorization is stripped on the way out: the caller already passed this
process's door, and UC OSS is a local sidecar, not a Databricks workspace.

## What is proxied

Catalog, schema, and **EXTERNAL** table CRUD — whatever UC OSS speaks at the
same path. This process does not reimplement the catalog.

```bash
curl -s -X POST "$HOST/api/2.1/unity-catalog/catalogs" \
  -H "Authorization: Bearer $PAT" -H "Content-Type: application/json" \
  -d '{"name":"main"}'
curl -s -X POST "$HOST/api/2.1/unity-catalog/tables" \
  -H "Authorization: Bearer $PAT" -H "Content-Type: application/json" \
  -d '{"name":"t","catalog_name":"main","schema_name":"default","table_type":"EXTERNAL","storage_location":"file:///tmp/t"}'
```

Witness: `ci:e2e-uc` — unmodified `databricks-sdk` creates catalog `e2e`, schema
`s`, and an EXTERNAL Delta table, then `tables.get` returns it. `make e2e-uc`
attaches `unitycatalog/unitycatalog:v0.5.0`. MANAGED create and grants stay
501 even with the sidecar.

## What is refused here, even with a sidecar

| Call | Why |
|---|---|
| `table_type=MANAGED` on `POST …/tables` | UC OSS only creates EXTERNAL tables at a filesystem location. Inventing a managed table Spark cannot see is a lookalike. 501. |
| `/permissions` and `/grants` | Not shipped until they deny. 501. |

Volumes, functions, locations, credentials, monitors, Delta Sharing — the
sidecar must speak them. Until a witness exists they stay 🔴 Not implemented on the
[parity ledger](parity.md).

`DATABRICKS_UC_TLS_INSECURE` skips TLS verification when dialing a
self-signed OSS. See [Configuration](04-configuration.md).
