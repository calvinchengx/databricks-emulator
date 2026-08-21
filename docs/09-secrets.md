# 09 — Secrets

Two backends. Databricks-backed scopes persist under `{dataDir}/secrets/`.
Azure Key Vault-backed scopes are a **live read-through** of a vault — there
is no sync. `GET /api/2.0/secrets/get` is refused on both: values resolve
only into a job, the way Databricks documents.

Secret ACLs are 501. Grants that would not be enforced are not shipped.

## Databricks-backed

```python
w.secrets.create_scope(scope="kv")
w.secrets.put_secret(scope="kv", key="pw", string_value="s3cret")
w.secrets.list_secrets(scope="kv")   # keys only
# w.secrets.get_secret(...)          # 400
```

Survive process restart with the same `DATABRICKS_DATA_DIR`. Witness:
`ci:e2e-sdk`.

`{{secrets/scope/key}}` in a task's `spark_env_vars` is resolved **in this
process** before the engine runs. Missing key → the run is `FAILED`. That miss
is in `ci:e2e-engine`.

**Resolution is not delivery, and only one of the two is a witness.** The
resolved value reaches the task exactly one way: `os.environ.update(...)` baked
into the generated code, before the task's own first line. It used to *also*
travel as a `req.Env` field on the request, which the agent does not read — so
a test could assert the secret had been resolved, pass, and say nothing about
whether the task ever saw it. That field is gone; the preamble is the only
path, and `ci:e2e-engine` reads it back from the task's own output
(`SECRET=s3cret` in `get-output`).

`spark_conf` is **refused**, not resolved. Real Databricks applies it when it
creates the cluster; here the agent's Spark session already exists, so there is
no moment at which this process could apply it the way Databricks does. It was
accepted and dropped before — including the secrets resolved inside it.

`spark_env_vars` is likewise refused on the task kinds with no generated code
to carry it: `sql_task`, `condition_task`, `run_job_task` and `for_each_task`.
`notebook_task`, `spark_python_task` and `dbt_task` carry it.

## Azure Key Vault-backed

```json
{
  "scope": "kv",
  "scope_backend_type": "AZURE_KEYVAULT",
  "backend_azure_keyvault": {
    "dns_name": "https://keyvault-emulator:8444",
    "resource_id": "/subscriptions/…/vaults/emulator"
  }
}
```

`dns_name` must be an Azure Key Vault suffix (`.vault.azure.net` and the
sovereign/MHSM variants) or the one host in `DATABRICKS_AKV_VAULT_HOST`.
Anything else is refused by name — an arbitrary URL would be SSRF.

`put` and `delete` on an AKV scope are 400
(`Cannot write secrets to Azure KeyVault-backed scope`). Rotate the vault
secret; the next job run GETs it. List keys is a live vault list.

Without `DATABRICKS_AKV_VAULT_HOST`, an emulator `dns_name` fails at create.
Witness: `ci:e2e-engine` — `put` refused, first run prints `vault-one`,
rotate, next run prints `vault-two`.

## Vault-audience token

When `DATABRICKS_ENTRA_TOKEN_URL` is set, each vault GET carries a
client-credentials bearer with scope `https://vault.azure.net/.default`.
`DATABRICKS_ENTRA_CLIENT_ID` and `DATABRICKS_ENTRA_CLIENT_SECRET` are
required together with the URL. Empty URL: resolve stays unauthenticated
(stand-in vault / `make run`).

Witness: `ci:e2e-engine` — vault GET without bearer is 401; the job's
read-through uses the minted audience. See
[Family integration](14-family-integration.md).
