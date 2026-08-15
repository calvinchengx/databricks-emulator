# databricks-emulator

A clean-room, local emulator of a **Databricks workspace**, built as a peer of
[fabric-emulator](https://github.com/calvinchengx/fabric-emulator) and the rest
of the [Azure emulator family](https://github.com/calvinchengx/azure-emulators).

The bet is the same one the family already runs: **terminate the public REST,
attach a real engine, refuse what you cannot compute.**

Identity is Databricks-native: a **seeded admin PAT** and this process's own
**OIDC**. Entra is an optional federated issuer. `token=dev` is 401.

```bash
make doctor
make test
make witnesses
make run   # https://localhost:8447 — first run prints the admin PAT once
```

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host="https://localhost:8447", token=open("data/admin.pat").read().strip())
print(w.current_user.me().user_name)
```

Published image:

```bash
docker pull ghcr.io/calvinchengx/databricks-emulator:0.1.0
```

Family compose:

```bash
docker compose --profile databricks up   # :8447
```

See [doctrine](00-doctrine.md) and the [parity ledger](parity.md).
