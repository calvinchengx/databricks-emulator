"""Pure-unit tests for the resolver — no emulator, no network."""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import databricks_target
from databricks_target import Target, TargetError, target
from databricks_target.__main__ import main


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "DATABRICKS_TARGET",
        "DATABRICKS_HOST",
        "DATABRICKS_EMULATOR_URL",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE",
        "DATABRICKS_CATALOG",
        "DATABRICKS_SCHEMA_SILVER",
        "DATABRICKS_SCHEMA_GOLD",
        "DATABRICKS_SECRET_SCOPE",
        "DATABRICKS_DATA_DIR",
        "DATABRICKS_SPARK_CONNECT_URL",
        "DATABRICKS_UC_URL",
        "DATABRICKS_VAULT_URL",
        "VAULT_EMULATOR_URL",
        "AZURE_KEY_VAULT_URL",
        "DATABRICKS_TARGET_ALLOW_DESTRUCTIVE",
    ):
        monkeypatch.delenv(k, raising=False)
    databricks_target._cached = None


def test_emulator_defaults_are_zero_config():
    t = Target("emulator")
    assert t.is_emulator and not t.is_real
    assert t.host == "http://localhost:8447"
    assert t.catalog == "contoso"
    assert t.schema_silver == "silver"
    assert t.schema_gold == "gold"
    assert t.secret_scope == "contoso"
    assert t.vault_url == "https://localhost:8444"
    assert t.tls_verify is False
    assert t.seed_secrets_allowed is True
    assert t.managed_tables_supported is False
    assert t.grants_enforced is False
    assert t.engine_is_attached is False


def test_emulator_urls_overridable(monkeypatch):
    monkeypatch.setenv("DATABRICKS_EMULATOR_URL", "http://127.0.0.1:18457/")
    monkeypatch.setenv("DATABRICKS_CATALOG", "e2e")
    monkeypatch.setenv("DATABRICKS_SPARK_CONNECT_URL", "http://127.0.0.1:18104")
    t = Target("emulator")
    assert t.host == "http://127.0.0.1:18457"
    assert t.catalog == "e2e"
    assert t.engine_is_attached is True


def test_emulator_accepts_the_production_names(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "http://127.0.0.1:18457")
    monkeypatch.setenv("AZURE_KEY_VAULT_URL", "https://localhost:18444")
    t = Target("emulator")
    assert t.host == "http://127.0.0.1:18457"
    assert t.vault_url == "https://localhost:18444"


def test_emulator_specific_name_wins(monkeypatch):
    monkeypatch.setenv("DATABRICKS_EMULATOR_URL", "http://localhost:1111")
    monkeypatch.setenv("DATABRICKS_HOST", "http://localhost:2222")
    assert Target("emulator").host == "http://localhost:1111"


def test_bogus_target_rejected():
    with pytest.raises(TargetError, match=r"emulator.*real"):
        Target("staging")


def test_real_requires_host(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "contoso_warehouse")
    with pytest.raises(TargetError, match="DATABRICKS_HOST"):
        Target("real")


def test_real_refuses_localhost(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "http://localhost:8447")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "contoso_warehouse")
    with pytest.raises(TargetError, match="localhost"):
        Target("real")


def test_real_requires_warehouse_name(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    with pytest.raises(TargetError, match="DATABRICKS_WAREHOUSE"):
        Target("real")


def test_real_requires_token(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "contoso_warehouse")
    with pytest.raises(TargetError, match="DATABRICKS_TOKEN"):
        Target("real")


def test_real_defaults(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "contoso_warehouse")
    monkeypatch.setenv("AZURE_KEY_VAULT_URL", "https://kv.vault.azure.net")
    t = Target("real")
    assert t.is_real
    assert t.tls_verify is True
    assert t.seed_secrets_allowed is False
    assert t.managed_tables_supported is True
    assert t.grants_enforced is True
    assert t.engine_is_attached is True
    assert t.vault_url == "https://kv.vault.azure.net"
    assert t.token == "dapi-real"


def test_emulator_only_guard(monkeypatch):
    Target("emulator").emulator_only("seed secrets")
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "wh")
    with pytest.raises(TargetError, match="emulator-only"):
        Target("real").emulator_only("seed secrets")


def test_refuse_seed_secrets_on_real(monkeypatch):
    Target("emulator").refuse_seed_secrets()
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "wh")
    with pytest.raises(TargetError, match="seed_secrets"):
        Target("real").refuse_seed_secrets()


def test_destructive_gate(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "wh")
    t = Target("real")
    with pytest.raises(TargetError, match="ALLOW_DESTRUCTIVE"):
        t._guard_destructive("DELETE")
    monkeypatch.setenv("DATABRICKS_TARGET_ALLOW_DESTRUCTIVE", "1")
    t._guard_destructive("DELETE")
    Target("emulator")._guard_destructive("DELETE")


def test_three_part_name():
    t = Target("emulator")
    assert t.three_part("silver", "events") == "contoso.silver.events"


def test_target_reads_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TARGET", "emulator")
    assert target(fresh=True).is_emulator


def test_token_from_admin_pat(tmp_path, monkeypatch):
    (tmp_path / "admin.pat").write_text("seeded-pat\n", encoding="utf-8")
    monkeypatch.setenv("DATABRICKS_DATA_DIR", str(tmp_path))
    assert Target("emulator").token == "seeded-pat"


def test_token_env_wins_over_pat(tmp_path, monkeypatch):
    (tmp_path / "admin.pat").write_text("seeded-pat\n", encoding="utf-8")
    monkeypatch.setenv("DATABRICKS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATABRICKS_TOKEN", "from-env")
    assert Target("emulator").token == "from-env"


def test_missing_pat_is_actionable(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABRICKS_DATA_DIR", str(tmp_path))
    with pytest.raises(TargetError, match="admin.pat"):
        _ = Target("emulator").token


def test_env_emitter_emulator(capsys):
    assert main(["prog", "env", "emulator"]) == 0
    out = capsys.readouterr().out
    assert "export DATABRICKS_TARGET=emulator" in out
    assert "http://localhost:8447" in out


def test_show_real(monkeypatch, capsys):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-1.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-real")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE", "contoso_warehouse")
    assert main(["prog", "show", "real"]) == 0
    out = capsys.readouterr().out
    assert "real" in out
    assert "adb-1.azuredatabricks.net" in out


def test_warehouse_requires_a_name():
    with pytest.raises(TargetError, match="DATABRICKS_WAREHOUSE"):
        Target("emulator").warehouse()
