"""One toggle between databricks-emulator and a real Databricks workspace.

    from databricks_target import target
    t = target()                          # reads DATABRICKS_TARGET (default: emulator)
    w = t.workspace_client()              # host + token already set
    wh = t.warehouse("contoso_warehouse") # name -> id / http_path

Design: docs/21-real-databricks-toggle.md. User code holds *names* and
receives endpoints/credentials — it never branches on which target is
active. The resolver does, once, here.
"""
from __future__ import annotations

import os
from collections import namedtuple
from pathlib import Path

Warehouse = namedtuple("Warehouse", ["id", "name", "http_path"])


class TargetError(RuntimeError):
    """A target rule was violated (emulator-only feature under real, missing
    credentials, unguarded destructive call, ...)."""


def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _env_any(names, default=None):
    """First non-empty of `names`, in order.

    Emulator mode names its own knobs DATABRICKS_EMULATOR_URL /
    VAULT_EMULATOR_URL, but a consumer driving BOTH targets from one
    compose file writes the production names (DATABRICKS_HOST,
    AZURE_KEY_VAULT_URL), because real mode requires them. Accepting both
    is what lets such a consumer adopt this package without rewriting its
    environment contract — the emulator-specific name still wins.
    """
    for n in names:
        v = _env(n)
        if v is not None:
            return v
    return default


def _looks_local(host: str) -> bool:
    h = host.lower()
    return "localhost" in h or "127.0.0.1" in h or "0.0.0.0" in h


class Target:
    def __init__(self, name):
        if name not in ("emulator", "real"):
            raise TargetError(
                f"DATABRICKS_TARGET must be 'emulator' or 'real', got {name!r}"
            )
        self.name = name

        if self.is_emulator:
            host = _env_any(
                ("DATABRICKS_EMULATOR_URL", "DATABRICKS_HOST"),
                "http://localhost:8447",
            ).rstrip("/")
            self.host = host
            # Family certs are self-signed. Real mode verifies; no knob
            # turns verification off there.
            self.tls_verify = False
            self.catalog = _env("DATABRICKS_CATALOG", "contoso")
            self.schema_silver = _env("DATABRICKS_SCHEMA_SILVER", "silver")
            self.schema_gold = _env("DATABRICKS_SCHEMA_GOLD", "gold")
            self.warehouse_scope = _env("DATABRICKS_WAREHOUSE")
            self.secret_scope = _env("DATABRICKS_SECRET_SCOPE", "contoso")
            self.vault_url = _env_any(
                ("VAULT_EMULATOR_URL", "AZURE_KEY_VAULT_URL"),
                "https://localhost:8444",
            ).rstrip("/")
            self.data_dir = Path(_env("DATABRICKS_DATA_DIR", "./data"))
            self.spark_connect_url = _env("DATABRICKS_SPARK_CONNECT_URL")
            self.uc_url = _env("DATABRICKS_UC_URL")
            self.seed_secrets_allowed = True
            self.managed_tables_supported = False
            self.grants_enforced = False
            self.engine_is_attached = bool(self.spark_connect_url)
        else:
            host = _env("DATABRICKS_HOST")
            if not host:
                raise TargetError(
                    "DATABRICKS_TARGET=real requires DATABRICKS_HOST="
                    "https://adb-<id>.azuredatabricks.net"
                )
            host = host.rstrip("/")
            if _looks_local(host):
                raise TargetError(
                    "DATABRICKS_TARGET=real was given a localhost host "
                    f"({host}) — that is the emulator. Unset DATABRICKS_HOST "
                    "or point it at a real workspace."
                )
            self.host = host
            self.tls_verify = True
            self.catalog = _env("DATABRICKS_CATALOG", "contoso")
            self.schema_silver = _env("DATABRICKS_SCHEMA_SILVER", "silver")
            self.schema_gold = _env("DATABRICKS_SCHEMA_GOLD", "gold")
            self.warehouse_scope = _env("DATABRICKS_WAREHOUSE")
            if not self.warehouse_scope:
                raise TargetError(
                    "DATABRICKS_TARGET=real requires DATABRICKS_WAREHOUSE="
                    "<warehouse display name> — real mode resolves by name "
                    "and never creates a warehouse."
                )
            self.secret_scope = _env("DATABRICKS_SECRET_SCOPE", "contoso")
            self.vault_url = _env_any(("DATABRICKS_VAULT_URL", "AZURE_KEY_VAULT_URL"))
            self.data_dir = None
            self.spark_connect_url = None
            self.uc_url = None
            self.seed_secrets_allowed = False
            self.managed_tables_supported = True
            self.grants_enforced = True
            self.engine_is_attached = True
            if not _env("DATABRICKS_TOKEN"):
                raise TargetError(
                    "DATABRICKS_TARGET=real needs DATABRICKS_TOKEN "
                    "(a PAT or OIDC access token for the real workspace)."
                )

    @property
    def is_emulator(self):
        return self.name == "emulator"

    @property
    def is_real(self):
        return self.name == "real"

    @property
    def token(self) -> str:
        """The PAT / bearer this target authenticates with.

        Emulator: DATABRICKS_TOKEN if set, otherwise the seeded admin PAT
        written on first start under DATABRICKS_DATA_DIR/admin.pat.
        Real: DATABRICKS_TOKEN, required at construction.
        """
        tok = _env("DATABRICKS_TOKEN")
        if tok:
            return tok
        if self.is_real:
            raise TargetError("DATABRICKS_TARGET=real needs DATABRICKS_TOKEN")
        pat = (self.data_dir or Path("./data")) / "admin.pat"
        if not pat.is_file():
            raise TargetError(
                f"emulator PAT not found at {pat} — start databricks-emulator "
                "once (make run) or set DATABRICKS_TOKEN / DATABRICKS_DATA_DIR"
            )
        return pat.read_text(encoding="utf-8").strip()

    def emulator_only(self, feature):
        """Declare a spot that has no real-workspace counterpart (seed
        secrets, controllable clock). Raises under real."""
        if self.is_real:
            raise TargetError(
                f"{feature}: emulator-only — this does not exist on real Databricks"
            )

    def refuse_seed_secrets(self):
        """Call from the consumer's seed-secrets step. Real mode must not
        write fixture credentials into a customer's vault or secret scope."""
        if not self.seed_secrets_allowed:
            raise TargetError(
                "seed_secrets is emulator-only — on DATABRICKS_TARGET=real "
                "the secret scope already holds the customer's values"
            )

    def _guard_destructive(self, method):
        if (
            self.is_real
            and method.upper() == "DELETE"
            and _env("DATABRICKS_TARGET_ALLOW_DESTRUCTIVE")
            not in ("1", "true", "yes")
        ):
            raise TargetError(
                "DELETE against real Databricks blocked — set "
                "DATABRICKS_TARGET_ALLOW_DESTRUCTIVE=1 to allow destructive calls."
            )

    def workspace_client(self):
        """Unmodified databricks-sdk WorkspaceClient bound to this target."""
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.config import Config
        except ImportError as e:
            raise TargetError(
                "workspace_client() needs databricks-sdk: "
                "uv add 'databricks-target[sdk]'"
            ) from e
        return WorkspaceClient(
            config=Config(
                host=self.host,
                token=self.token,
                skip_verify=not self.tls_verify,
            )
        )

    def warehouse(self, name=None) -> Warehouse:
        """Resolve a SQL warehouse by display name on the active target.

        Names are the cross-target contract; warehouse ids never match
        across targets. This method does not create — provision is the
        consumer's job, the same way fabric-target.workspace() only
        resolves.
        """
        name = name or self.warehouse_scope
        if not name:
            raise TargetError(
                "warehouse(): pass a name or set DATABRICKS_WAREHOUSE"
            )
        w = self.workspace_client()
        for wh in w.warehouses.list():
            if getattr(wh, "name", None) == name or getattr(wh, "id", None) == name:
                wid = wh.id
                return Warehouse(wid, getattr(wh, "name", name), f"/sql/1.0/endpoints/{wid}")
        raise TargetError(
            f"warehouse {name!r} not found on target {self.name}"
        )

    def three_part(self, schema: str, table: str) -> str:
        """catalog.schema.table — Unity Catalog names, either target."""
        return f"{self.catalog}.{schema}.{table}"


_cached = None


def target(name=None, fresh=False):
    """The resolver. Reads DATABRICKS_TARGET unless a name is given."""
    global _cached
    if name is None and not fresh and _cached is not None:
        return _cached
    t = Target(name or _env("DATABRICKS_TARGET", "emulator"))
    if name is None:
        _cached = t
    return t
