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

`{{secrets/scope/key}}` in a task's `spark_env_vars` and `spark_conf` is
resolved **in this process** before the engine runs. Missing key → the run
is `FAILED`. That miss is in `ci:e2e-engine`.

Resolved values are sent as `req.Env` / `req.Conf`. The family's spark-agent
drops `env`, so a Python task also bakes `os.environ.update(...)` into the
driver preamble — that is the attach, not a lookalike. Witness:
`ci:e2e-engine` (`SECRET=s3cret` in `get-output`).

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
