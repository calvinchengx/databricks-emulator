#!/usr/bin/env python3
"""The databricks-target toggle resolves the emulator and drives a warehouse
by name. Consumer code holds names; the resolver turns them into ids.

    target() -> workspace_client() -> warehouse(name) -> SELECT 1
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
PKG = ROOT / "python" / "databricks-target"
HOST = "http://127.0.0.1:18457"
AGENT = "http://127.0.0.1:18104"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-target"]

sys.path.insert(0, str(PKG))


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
    env["DATABRICKS_ADDR"] = "127.0.0.1:18457"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    proc = subprocess.Popen(
        [str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    wait_http(HOST + "/health")
    return proc


def main() -> int:
    from databricks_target import TargetError, target

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-target-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)

        os.environ["DATABRICKS_TARGET"] = "emulator"
        os.environ["DATABRICKS_EMULATOR_URL"] = HOST
        os.environ["DATABRICKS_DATA_DIR"] = str(data_dir)
        os.environ["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
        os.environ["DATABRICKS_WAREHOUSE"] = "contoso_warehouse"

        t = target(fresh=True)
        if not t.is_emulator:
            raise SystemExit(f"expected emulator, got {t.name}")
        if t.tls_verify:
            raise SystemExit("emulator must not verify TLS")
        if not t.seed_secrets_allowed:
            raise SystemExit("emulator must allow seed_secrets")
        if t.managed_tables_supported:
            raise SystemExit("emulator must not claim MANAGED tables")

        w = t.workspace_client()
        created = w.warehouses.create(name="contoso_warehouse").result()
        if not created.id:
            raise SystemExit("warehouse create returned no id")

        wh = t.warehouse("contoso_warehouse")
        if wh.id != created.id:
            raise SystemExit(f"warehouse() resolved {wh.id}, create returned {created.id}")
        if wh.http_path != f"/sql/1.0/endpoints/{created.id}":
            raise SystemExit(f"http_path {wh.http_path}")

        scoped = t.warehouse()
        if scoped.id != wh.id:
            raise SystemExit("DATABRICKS_WAREHOUSE scope did not resolve the same warehouse")

        stmt = w.statement_execution.execute_statement(
            warehouse_id=wh.id, statement="SELECT 1 AS n"
        )
        state = stmt.status.state.value if stmt.status and stmt.status.state else None
        if state not in {"SUCCEEDED", "SUCCESS"}:
            raise SystemExit(f"SELECT 1: state={state} {stmt}")

        try:
            t.warehouse("no-such-warehouse")
        except TargetError as exc:
            if "no-such-warehouse" not in str(exc):
                raise SystemExit(f"missing-warehouse error was unhelpful: {exc}")
        else:
            raise SystemExit("warehouse() accepted a name that does not exist")

        print(
            f"databricks-target: resolved {t.host} warehouse {wh.name} "
            f"id={wh.id} path={wh.http_path}; SELECT 1 {state}"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
