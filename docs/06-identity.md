# 06 — Identity

Three doors. PAT and this process's own OIDC always work. Federated JWT works
when `DATABRICKS_OIDC_ISSUERS` is set. `"dev"` is 401 unless that exact value
was minted as a PAT — the seeder will not.

## PAT

First start writes a seeded admin PAT to `{dataDir}/admin.pat` (`0600`) and
prints it once. The value starts with `dapi`, as real Databricks PATs do;
Bearer dispatch uses that prefix. The store keeps only a hash in
`identity.json`. The username is `admin`.

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host="http://127.0.0.1:8447", token=open("data/admin.pat").read().strip())
print(w.current_user.me().user_name)
```

Unknown PATs and `token=dev` are 401. The WWW-Authenticate challenge names
this process's OIDC origin.

Witness: `ci:e2e-sdk`, `ci:e2e-terraform`.

## Emulator OIDC

This process is its own authorization server. No entra-emulator required.

| Path | Role |
|---|---|
| `GET /.well-known/databricks-config` | `{oidc_endpoint: {origin}/oidc, workspace_id: "1"}` — what `databricks-sdk` reads to find OIDC |
| `GET /oidc/.well-known/openid-configuration` | Discovery |
| `GET /oidc/.well-known/oauth-authorization-server` | RFC 8414 alias of the same document |
| `GET /oidc/jwks.json` | Signing keys |
| `POST /oidc/v1/token` | `client_credentials` only (`client_secret_post` or `client_secret_basic`) |

The seeded confidential client is `databricks-emulator-client`. The secret is
written to `{dataDir}/oidc-client.json` on first boot and printed once with
the PAT. The RSA signing key is persisted under `{dataDir}/oidc/`.

```python
import json
from databricks.sdk import WorkspaceClient

cred = json.loads(open("data/oidc-client.json").read())
w = WorkspaceClient(
    host="http://127.0.0.1:8447",
    client_id=cred["client_id"],
    client_secret=cred["client_secret"],
    auth_type="oauth-m2m",
)
print(w.current_user.me().user_name)  # -> admin
```

Tokens are RS256. Accepted audiences are the advertised origin
(`DATABRICKS_PUBLIC_URL`) and the well-known Azure Databricks app id
`2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`.

Witness: `ci:e2e-sdk`.

## Federated JWT

Opt-in. `DATABRICKS_OIDC_ISSUERS` is a comma-separated list of issuer URLs.
Unconfigured, a foreign JWT is 401.

JWKS fetch:

- issuer ending in `/v2.0` (entra) → `{issuer minus /v2.0}/discovery/v2.0/keys`
- otherwise → `{issuer}/jwks.json`

Validation is RS256 against that JWKS, issuer match, audience in {origin,
emulator OIDC issuer, Azure Databricks app id}, `exp`/`nbf` with 60s skew.
Wrong audience, expired token, and `aaa.bbb.ccc` are 401. The principal is
`preferred_username`, else `unique_name`, else `sub`, else `oid`.

Family compose sets the issuer to entra-emulator's v2.0 URL and
`DATABRICKS_OIDC_TLS_INSECURE=true` so the self-signed JWKS fetch works.
The chain test mints a Databricks-audience token
(`2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`) and calls `/Me`.

Witness: `ci:e2e-sdk`.
