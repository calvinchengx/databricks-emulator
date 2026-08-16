# 04 — Configuration

Every setting has a `DATABRICKS_*` environment variable. Flags override when
both are set, except `DATABRICKS_OIDC_ISSUERS`, which is environment-only.

Nothing is required. `make run` with an empty environment serves HTTPS on
`:8447`, seeds identity under `./data`, and refuses Jobs until an engine URL
is set.

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--addr` | `DATABRICKS_ADDR` | `:8447` | Listen address. |
| `--data-dir` | `DATABRICKS_DATA_DIR` | `./data` | State directory: identity, TLS, OIDC key, workspace, DBFS, secrets. The distroless image sets `/data`. |
| `--public-url` | `DATABRICKS_PUBLIC_URL` | derived | Advertised origin for OIDC discovery and `/.well-known/databricks-config`. Derived as `https://localhost{addr}` (or `http://` when TLS is off) when unset. Set this on compose so tokens carry the in-network issuer. |
| `--disable-tls` | `DATABRICKS_DISABLE_TLS` | off | Serve plain HTTP. REST stays HTTP/1; Databricks Connect uses h2c prior knowledge on the same port. Truthy: `1`, `true`, `yes`, `on`. |
| `--spark-connect-url` | `DATABRICKS_SPARK_CONNECT_URL` | *(unset)* | Statement-agent origin Jobs, SQL, and cluster-create drive (`{url}/statements`). Empty: those paths fail naming the missing engine. |
| `--spark-connect-grpc-url` | `DATABRICKS_SPARK_CONNECT_GRPC_URL` | *(unset)* | Spark Connect gRPC origin Databricks Connect is reverse-proxied to (Sail `:50051`). An HTTP agent is not this URL. Empty: Connect is 501 naming this variable. |
| — | `DATABRICKS_OIDC_ISSUERS` | *(unset)* | Comma-separated federated issuer URLs (entra or any OIDC). Empty: only PAT and this process's OIDC work. No flag. |
| `--oidc-tls-insecure` | `DATABRICKS_OIDC_TLS_INSECURE` | off | Skip TLS verification when fetching federated JWKS (entra-emulator's self-signed cert). Also used when minting a vault-audience token. |
| `--akv-vault-host` | `DATABRICKS_AKV_VAULT_HOST` | *(unset)* | The one non-Azure `host:port` accepted as a Key Vault (keyvault-emulator). Empty: only `*.vault.azure.net` suffixes are allowlisted, so an emulator `dns_name` fails by name. |
| `--akv-tls-insecure` | `DATABRICKS_AKV_TLS_INSECURE` | off | Skip TLS verification when dialing the vault. |
| `--uc-url` | `DATABRICKS_UC_URL` | *(unset)* | Unity Catalog OSS origin. Empty: `/unity-catalog` routes are 501 naming the missing sidecar. |
| `--uc-tls-insecure` | `DATABRICKS_UC_TLS_INSECURE` | off | Skip TLS verification when dialing UC OSS. |
| — | `DATABRICKS_DELTA_ROOT` | `file:///data/delta/managed` | Engine-visible URI prefix for warehouse `CREATE TABLE cat.sch.t` with no `LOCATION`. |
| `--entra-token-url` | `DATABRICKS_ENTRA_TOKEN_URL` | *(unset)* | Client-credentials endpoint used to mint a vault-audience token (`https://vault.azure.net/.default`) for AKV-backed secret resolve. Empty: resolve stays unauthenticated (stand-in vault / `make run`). |
| `--entra-client-id` | `DATABRICKS_ENTRA_CLIENT_ID` | *(unset)* | Confidential client id for that mint. Required together with the secret when the token URL is set. |
| `--entra-client-secret` | `DATABRICKS_ENTRA_CLIENT_SECRET` | *(unset)* | Confidential client secret for that mint. |

## Derived origin

`--public-url` is the issuer the emulator OIDC advertises
(`{origin}/oidc`) and the audience this process accepts on its own tokens.
On a compose network set it to the in-network URL
(`https://databricks-emulator:8447`); otherwise tokens minted against
`localhost` fail when a peer validates `iss`.

## Docker environment

The distroless image sets `DATABRICKS_DATA_DIR=/data` and exposes `8447`.
Mount `/data` to keep the PAT, TLS fingerprint, and workspace across restarts.
`databricks-emulator healthcheck` is the image `HEALTHCHECK`.

Family compose in azure-emulators wires federated Entra, the vault host, and
the vault-audience mint. It does not set `--spark-connect-url`. See
[Family integration](14-family-integration.md).

## Client toggle (not this binary)

A consumer that must also run against a real workspace does not restate
these URLs. It installs the published `databricks-target` package and
sets `DATABRICKS_TARGET=emulator|real`. See
[21 — one toggle](21-real-databricks-toggle.md).

## What is not configured here

- The seeded admin PAT and OIDC client are written on first start under the
  data dir, not passed as flags. See [Identity](06-identity.md).
- Sail's own settings (`SPARK_REMOTE`, session timeout) belong to the
  spark-agent / Sail containers, not this binary.
