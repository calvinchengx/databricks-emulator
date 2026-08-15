# 01 — Quickstart

Bring up the workspace, read the seeded PAT, and call `Me` with the official
SDK. `token=dev` is 401. No Spark sidecar is required for this path.

## 1. Run it

```bash
make doctor
DATABRICKS_DISABLE_TLS=1 make run
```

First boot prints the admin PAT once and writes it to `data/admin.pat`. Later
boots reuse that file; they do not print it again. HTTP is the one-minute
path — TLS is on by default, documented in [TLS and hosts](05-tls-and-hosts.md).

## 2. Call Me with the official SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host="http://127.0.0.1:8447",
    token=open("data/admin.pat").read().strip(),
)
print(w.current_user.me().user_name)  # -> admin
```

That is unmodified `databricks-sdk`. The same PAT is what the Databricks CLI
and `databricks/databricks` Terraform send.

## 3. Prove the door

```bash
curl -s http://127.0.0.1:8447/api/2.0/preview/scim/v2/Me \
  -H "Authorization: Bearer $(cat data/admin.pat)"
# {"userName":"admin", ...}

curl -s -o /dev/null -w "%{http_code}\n" \
  http://127.0.0.1:8447/api/2.0/preview/scim/v2/Me \
  -H "Authorization: Bearer dev"
# 401
```

`"dev"` is 401 unless that exact value was minted as a PAT. The seeder will
not mint it. Any-token-accepted is not identity — see
[Identity](06-identity.md).

## What this did not start

Jobs, SQL warehouses, cluster create, and MCP SQL need an attached Spark
engine (`DATABRICKS_SPARK_CONNECT_URL`). Without one they fail naming the
missing engine — they never report `SUCCESS`. Attach it with
[Jobs and the Spark attach](08-jobs-and-spark.md), or run `make e2e-engine`.

Family compose (`docker compose --profile databricks up` in
[azure-emulators](https://github.com/calvinchengx/azure-emulators)) brings
entra and keyvault along. It does not attach Sail. See
[Family integration](14-family-integration.md).
