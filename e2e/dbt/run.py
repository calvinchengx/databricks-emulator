#!/usr/bin/env python3
"""Drive unmodified dbt-databricks against the warehouse HiveServer2 attach,
then confirm the models with delta-rs.

This is dbt talking to the warehouse, not Jobs dbt_task (still refused).

dbt exiting 0 is not a witness. The models are materialized onto a mounted
volume as external Delta tables, and delta-rs -- which never spoke to dbt or
to Sail -- reads the log and the rows back. The engine that wrote is not the
one that confirms.
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
HOST = "http://127.0.0.1:18456"
AGENT = "http://127.0.0.1:18103"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-dbt"]


def wait_http(url: str, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
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
    env["DATABRICKS_ADDR"] = "127.0.0.1:18456"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_http(HOST + "/health")
    return proc


def api(method: str, path: str, token: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        HOST + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else {}


def write_profiles(profiles_dir: Path, token: str, http_path: str, uri: str) -> None:
    # Catalog defaults to hive_metastore inside the adapter. _connection_uri
    # is required — the connector prefixes https:// otherwise. host includes
    # the scheme so the SDK PAT path does not hang on :443.
    profiles = {
        "emulator": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "databricks",
                    "host": "http://127.0.0.1:18456",
                    "http_path": http_path,
                    "token": token,
                    "schema": "default",
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


def main() -> int:
    data_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    host_root = Path(tempfile.mkdtemp(prefix="dbx-dbt-tables-"))
    os.chmod(host_root, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    env = os.environ.copy()
    env["DBT_DATA"] = str(host_root)

    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        wh = api("POST", "/api/2.0/sql/warehouses", pat, {"name": "e2e-dbt"})
        wid = wh.get("id")
        if not wid:
            raise SystemExit(f"warehouse id missing: {wh!r}")

        path = f"/sql/1.0/endpoints/{wid}"
        uri = f"http://127.0.0.1:18456{path}"
        profiles_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-profiles-"))
        project_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-project-"))
        shutil.copytree(PROJECT, project_dir, dirs_exist_ok=True)
        write_profiles(profiles_dir, pat, path, uri)

        dbt(["debug"], project_dir, profiles_dir)
        # two reads one via ref() on the same dbt thread/session — Sail's
        # memory catalog is not visible to a later connector session.
        dbt(["run", "--select", "one", "two"], project_dir, profiles_dir)

        # The witness: delta-rs reads what Sail wrote, without asking dbt.
        v1 = confirm(host_root / "one", [(1,)], "one")
        v2 = confirm(host_root / "two", [(1,)], "two")
        print(f"   delta-rs confirmed one (v{v1}) and two (v{v2}) via ref()")

        bad_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-bad-"))
        write_profiles(bad_dir, "dev", path, uri)
        try:
            dbt(["debug"], project_dir, bad_dir)
        except subprocess.CalledProcessError:
            print("   token=dev refused")
        else:
            raise SystemExit("token=dev was accepted by dbt-databricks")

        print("unmodified dbt-databricks 1.12.4 materialized one+two; delta-rs confirmed the rows")
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
