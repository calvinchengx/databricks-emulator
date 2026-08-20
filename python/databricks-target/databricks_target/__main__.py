"""The env emitter — the toggle for tools that only read environment.

    eval "$(python -m databricks_target env emulator)"
    eval "$(python -m databricks_target env real)"
    python -m databricks_target show
"""
from __future__ import annotations

import shlex
import sys

from . import Target, TargetError, target as resolve


def _pairs(t: Target):
    rows = [
        ("DATABRICKS_TARGET", t.name),
        ("DATABRICKS_HOST", t.host),
        ("DATABRICKS_CATALOG", t.catalog),
        ("DATABRICKS_SCHEMA_SILVER", t.schema_silver),
        ("DATABRICKS_SCHEMA_GOLD", t.schema_gold),
        ("DATABRICKS_SECRET_SCOPE", t.secret_scope),
    ]
    if t.warehouse_scope:
        rows.append(("DATABRICKS_WAREHOUSE", t.warehouse_scope))
    if t.vault_url:
        rows.append(("AZURE_KEY_VAULT_URL", t.vault_url))
    if t.is_emulator and t.spark_connect_url:
        rows.append(("DATABRICKS_SPARK_CONNECT_URL", t.spark_connect_url))
    if t.is_emulator and t.uc_url:
        rows.append(("DATABRICKS_UC_URL", t.uc_url))
    return rows


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "show"
    name = argv[2] if len(argv) > 2 else None
    try:
        t = Target(name) if name else resolve()
    except TargetError as err:
        print(f"# databricks_target: {err}", file=sys.stderr)
        return 1

    if cmd == "env":
        for k, v in _pairs(t):
            if v is None:
                continue
            print(f"export {k}={shlex.quote(str(v))}")
        print(f"# databricks_target: profile '{t.name}' — host {t.host}")
        if t.is_real:
            print("# credentials: DATABRICKS_TOKEN (not emitted)")
        return 0
    if cmd == "show":
        for k, v in [
            ("target", t.name),
            ("host", t.host),
            ("catalog", t.catalog),
            ("warehouse", t.warehouse_scope or "—"),
            ("secret_scope", t.secret_scope),
            ("vault", t.vault_url or "—"),
            ("tls_verify", str(t.tls_verify)),
            ("seed_secrets", str(t.seed_secrets_allowed)),
            ("managed_tables", str(t.managed_tables_supported)),
            ("grants_enforced", str(t.grants_enforced)),
        ]:
            print(f"{k:16} {v}")
        return 0
    print(
        "usage: python -m databricks_target [env|show] [emulator|real]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
