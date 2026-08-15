# Security

## Reporting a vulnerability

Report privately through GitHub, on this repository:
**[Security → Report a vulnerability](https://github.com/calvinchengx/databricks-emulator/security/advisories/new)**.

Do not open a public issue for a security report.

## What this project is

**databricks-emulator is a local development tool.** It seeds an admin PAT
into `./data/` on first run, serves self-signed TLS by default, and is meant
to run on `localhost`. It is not a Databricks workspace and must never hold
a real customer secret.

Identity is the product: unknown bearers and `token=dev` are 401. Defects in
PAT hashing, OIDC signature checks, or federated `aud`/`iss`/`exp` validation
are in scope.

### In scope

- Token validation that is wrong rather than absent (tampered PAT/JWT accepted,
  skipped signature, unenforced audience or expiry).
- Path traversal on workspace, DBFS, or Files API that escapes `data/`.
- SSRF via an AKV `dns_name` or UC sidecar URL that is not allowlisted.

### Out of scope

- Running this process on a public network.
- Secrets persisted under `data/secrets/` with no encryption at rest.
- The seeded admin PAT and OIDC client secret (intentionally insecure local seeds).
