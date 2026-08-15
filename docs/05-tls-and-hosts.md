# 05 — TLS and hosts

TLS is on by default. The certificate is self-signed, persisted, and stable
across restarts so a fingerprint you trusted yesterday still matches.

## Certificate

Generated on first HTTPS boot, then reused from `{dataDir}/tls/cert.pem` and
`key.pem` (key is `0600`). ECDSA P-256; CN = `databricks-emulator`; ten-year
validity.

SANs:

- `localhost`, `databricks-emulator`
- `azuredatabricks.net`, `*.azuredatabricks.net`
- `127.0.0.1`, `::1`

A hosts-file pin of an `adb-*.azuredatabricks.net` name at `127.0.0.1` therefore
verifies the name. There is no local CA, no `trust --apply`, and no cert
hot-reload — delete `data/tls/` to rotate.

## Plain HTTP

```bash
DATABRICKS_DISABLE_TLS=1 make run
# or: databricks-emulator --disable-tls
```

This is what [Quickstart](01-quickstart.md) and the SDK / engine e2e jobs use.
Clients then talk `http://127.0.0.1:8447` with no verify flag.

## Clients against HTTPS

```bash
curl -sk https://localhost:8447/health
```

The official Python SDK verifies TLS. Point it at HTTP for local loops, or
install `data/tls/cert.pem` in a trust store and use a SAN the cert covers.
`DATABRICKS_PUBLIC_URL` must match the origin the client uses, because OIDC
discovery advertises that origin.

## Healthcheck pins the cert

`databricks-emulator healthcheck` (the distroless `HEALTHCHECK`) reads the
already-persisted PEM and uses it as `RootCAs` against `127.0.0.1`. It never
generates a cert and never sets `InsecureSkipVerify`. A probe that skipped
verification would make the container healthcheck a MITM target.

## Federated and sidecar TLS

Self-signed peers on a compose network need the matching `*_TLS_INSECURE`
knob: `DATABRICKS_OIDC_TLS_INSECURE` when fetching entra JWKS (and when
minting a vault-audience token), `DATABRICKS_AKV_TLS_INSECURE` when dialing
the vault, `DATABRICKS_UC_TLS_INSECURE` when dialing UC OSS. Those skip
verification on **outbound** calls this process makes. They do not change
what this process serves.
