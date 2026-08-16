# 21 — Design: one toggle between databricks-emulator and a real workspace

**Status: shipped** (`python/databricks-target/`, CI `e2e-databricks-target`).
A consumer's Python — `databricks-sdk`, `dbt-databricks`, warehouse SQL —
runs against **either** the local emulator **or** a real Databricks
workspace, switched by **one setting**, with zero code edits.

This is the sibling of fabric-emulator's `fabric-target`
(`FABRIC_TARGET=emulator|real`). Contoso's rule is "the toggle contract is
installed, never restated." While this package was unpublished a consumer
could only copy hosts and PATs, which is how workspace ids and seeded
secrets leak into production.

## The contract

One switch: `DATABRICKS_TARGET=emulator | real` (default `emulator`).

| Resolved value | `emulator` (zero-config defaults) | `real` (from standard env) |
|---|---|---|
| Host | `http://localhost:8447` | `DATABRICKS_HOST` (`https://adb-*.azuredatabricks.net`) |
| Credential | seeded admin PAT (`DATABRICKS_DATA_DIR/admin.pat`) or `DATABRICKS_TOKEN` | `DATABRICKS_TOKEN` (required) |
| SQL warehouse | by **name**, resolved to `wh-*` / `/sql/1.0/endpoints/{id}` | by **name** (`DATABRICKS_WAREHOUSE`, required) — never created here |
| Catalog / schemas | `contoso` / `silver` / `gold` | same names, override with `DATABRICKS_CATALOG` |
| Secret scope | `contoso` | same name |
| Key Vault | azure-keyvault-emulator (`https://localhost:8444`) | `AZURE_KEY_VAULT_URL` |
| TLS verify | off (self-signed family certs, or plain HTTP) | on — no knob turns it off |
| Engine attach | `DATABRICKS_SPARK_CONNECT_URL` + optional `DATABRICKS_UC_URL` | the workspace *is* the engine |
| Seed secrets | allowed | refused |
| MANAGED tables | not supported (UC OSS EXTERNAL only) | supported |
| Grants | not enforced (501 until they deny) | enforced |

Ids are the one thing that can never match across targets — so the
contract is **name-based**: user code holds warehouse / catalog / schema /
scope display names; the resolver translates to ids per target.

### Variable names, and why there are two sets

Emulator-mode knobs are named `DATABRICKS_EMULATOR_URL` /
`VAULT_EMULATOR_URL`; real mode reads `DATABRICKS_HOST` and
`AZURE_KEY_VAULT_URL`. A consumer driving **both** targets from one
compose file writes the production names, because real mode leaves it no
choice — so emulator mode accepts them as aliases. The emulator-specific
name still wins.

| Resolved value | Preferred | Also accepted |
|---|---|---|
| Workspace host | `DATABRICKS_EMULATOR_URL` | `DATABRICKS_HOST` |
| Key Vault | `VAULT_EMULATOR_URL` | `AZURE_KEY_VAULT_URL` |

Real mode **refuses a localhost host**. A shell left over from `make run`
must not silently talk to the emulator while believing it is production.

## Consumer code

```python
from databricks_target import target

t = target()                          # reads DATABRICKS_TARGET
w = t.workspace_client()              # host + token already set
wh = t.warehouse("contoso_warehouse") # name -> id / http_path
name = t.three_part("gold", "fct_revenue_summary")
```

No `if t.name == "emulator"` in the consumer. Policy flags that are
genuinely different live on the Target object:

- `t.seed_secrets_allowed` — call `t.refuse_seed_secrets()` from the
  seed step
- `t.managed_tables_supported` — emulator is EXTERNAL only
- `t.grants_enforced` — emulator grants stay 501
- `t.engine_is_attached` — emulator is false until
  `DATABRICKS_SPARK_CONNECT_URL` is set

`warehouse()` resolves. It does not create. Provision is the consumer's
job, the same way `fabric-target.workspace()` only resolves.

## Witness

`make e2e-databricks-target` starts this binary plus Sail, resolves the
emulator profile, creates a warehouse named `contoso_warehouse` (the
consumer half), resolves it by name through the package, and runs
`SELECT 1`. Real-target conformance is secret-gated
(`.github/workflows/real-databricks.yml`) and is not a PR check.

## What this is not

- A Databricks Runtime. Photon / DBR claims stay refused.
- A substitute for `DATABRICKS_*` on the *server*. This package is the
  **client** resolver. The binary still reads
  [04-configuration.md](04-configuration.md).
- OpenMetadata. Governance compose belongs to the consumer, as
  contoso-fabric-platform owns its own `governance` profile.
