#!/usr/bin/env python3
"""Unmodified dbt-databricks against a Unity Catalog catalog over HiveServer2.

This is the gold-shaped gate: catalog set, +file_format delta, no post-hook,
no location_root. The warehouse shim turns CREATE TABLE cat.sch.t (no
LOCATION) into an EXTERNAL path Sail can write. dbt exiting 0 is not a
witness — delta-rs reads the managed volume, then a three-part SELECT
runs through the warehouse.

hive_metastore dbt (e2e/dbt) stays a Thrift smoke. Neither suite covers
Jobs dbt_task; e2e/dbt-task does, by running dbt inside the agent.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
PROJECT = HERE / "project"
HOST = "http://127.0.0.1:18457"
AGENT = "http://127.0.0.1:18104"
UC = "http://127.0.0.1:18453"
CATALOG = "e2e"
SCHEMA = "gold"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-dbt-uc"]


def wait_http(url: str, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError, urllib.error.HTTPError) as exc:
            last = exc
            time.sleep(0.2)
    raise SystemExit(f"never healthy at {url}: {last}")


def stop(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def start_emulator(bin_path: Path, data_dir: Path) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data_dir)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18457"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    env["DATABRICKS_UC_URL"] = UC
    env["DATABRICKS_DELTA_ROOT"] = "file:///data/delta/managed"
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_http(HOST + "/health")
    return proc


def api(method: str, path: str, token: str, body: dict | None = None, ok: tuple[int, ...] = (200, 201)) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        HOST + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            code = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        code = exc.code
        if code not in ok:
            raise SystemExit(f"{method} {path}: {code} {raw.decode(errors='replace')}")
    else:
        if code not in ok:
            raise SystemExit(f"{method} {path}: {code} {raw.decode(errors='replace')}")
    return json.loads(raw) if raw else {}


def write_profiles(profiles_dir: Path, token: str, http_path: str, uri: str) -> None:
    # catalog is set: this is the gold shape, not hive_metastore. host includes
    # the scheme so the SDK PAT path does not hang on :443. _connection_uri
    # is required — the connector prefixes https:// otherwise.
    profiles = {
        "emulator": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "databricks",
                    "host": "http://127.0.0.1:18457",
                    "http_path": http_path,
                    "token": token,
                    "catalog": CATALOG,
                    "schema": SCHEMA,
                    "threads": 1,
                    "connect_retries": 1,
                    "connect_timeout": 20,
                    "connection_parameters": {
                        "_connection_uri": uri,
                        "use_cloud_fetch": False,
                        "enable_telemetry": False,
                        "_retry_stop_after_attempts_count": 3,
                        "_retry_delay_max": 1,
                        "_socket_timeout": 30,
                    },
                }
            },
        }
    }
    (profiles_dir / "profiles.yml").write_text(yaml.safe_dump(profiles), encoding="utf-8")


def dbt(args: list[str], project_dir: Path, profiles_dir: Path) -> None:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    cmd = ["dbt", *args, "--project-dir", str(project_dir), "--profiles-dir", str(profiles_dir)]
    subprocess.check_call(cmd, cwd=project_dir, env=env)


def confirm(table_dir: Path, want_rows: list[tuple[int]], model: str) -> int:
    """Read a materialized model with delta-rs. dbt is not consulted."""
    from deltalake import DeltaTable

    log = table_dir / "_delta_log"
    if not log.is_dir():
        listing = list(table_dir.rglob("*")) if table_dir.exists() else []
        raise SystemExit(
            f"dbt reported success but {model} has no _delta_log at {table_dir}: {listing[:20]}"
        )
    dt = DeltaTable(str(table_dir))
    table = dt.to_pyarrow_table()
    cols = {c.lower(): c for c in table.column_names}
    if "id" not in cols:
        raise SystemExit(f"{model}: delta-rs columns {list(table.column_names)} missing id")
    got = sorted((int(v),) for v in table.column(cols["id"]).to_pylist())
    if got != sorted(want_rows):
        raise SystemExit(f"{model}: delta-rs rows {got} != {sorted(want_rows)}")
    return dt.version()


def warehouse_select(pat: str, warehouse_id: str, statement: str) -> str:
    st = api("POST", "/api/2.0/sql/statements", pat, {"warehouse_id": warehouse_id, "statement": statement})
    state = (st.get("status") or {}).get("state")
    if state not in {"SUCCEEDED", "SUCCESS"}:
        raise SystemExit(f"select {statement!r}: {st!r}")
    return str((st.get("result") or {}).get("text") or "")


def main() -> int:
    data_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-uc-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    host_root = Path(tempfile.mkdtemp(prefix="dbx-dbt-uc-delta-"))
    os.chmod(host_root, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    env = os.environ.copy()
    env["DELTA_DATA"] = str(host_root)

    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        wait_http(UC + "/api/2.1/unity-catalog/catalogs", timeout=120)
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        api("POST", "/api/2.1/unity-catalog/catalogs", pat, {"name": CATALOG}, ok=(200, 201, 409))
        api(
            "POST",
            "/api/2.1/unity-catalog/schemas",
            pat,
            {"name": SCHEMA, "catalog_name": CATALOG},
            ok=(200, 201, 409),
        )
        wh = api("POST", "/api/2.0/sql/warehouses", pat, {"name": "e2e-dbt-uc"})
        wid = wh.get("id")
        if not wid:
            raise SystemExit(f"warehouse id missing: {wh!r}")

        path = f"/sql/1.0/endpoints/{wid}"
        uri = f"http://127.0.0.1:18457{path}"
        profiles_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-uc-profiles-"))
        project_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-uc-project-"))
        shutil.copytree(PROJECT, project_dir, dirs_exist_ok=True)
        write_profiles(profiles_dir, pat, path, uri)

        dbt(["debug"], project_dir, profiles_dir)
        dbt(["run", "--select", "one", "two"], project_dir, profiles_dir)

        managed = host_root / "managed" / CATALOG / SCHEMA
        v1 = confirm(managed / "one", [(1,)], "one")
        v2 = confirm(managed / "two", [(1,)], "two")
        print(f"   delta-rs confirmed {CATALOG}.{SCHEMA}.one (v{v1}) and two (v{v2})")

        text = warehouse_select(pat, wid, f"SELECT id FROM {CATALOG}.{SCHEMA}.two")
        if "1" not in text:
            raise SystemExit(f"three-part SELECT did not return 1: {text!r}")
        print(f"   warehouse SELECT {CATALOG}.{SCHEMA}.two -> {text}")

        bad_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-uc-bad-"))
        write_profiles(bad_dir, "dev", path, uri)
        try:
            dbt(["debug"], project_dir, bad_dir)
        except subprocess.CalledProcessError:
            print("   token=dev refused")
        else:
            raise SystemExit("token=dev was accepted by dbt-databricks")

        print("unmodified dbt-databricks 1.12.4 materialized e2e.gold.one+two; delta-rs confirmed")
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
