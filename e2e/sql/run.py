#!/usr/bin/env python3
"""Drive unmodified databricks-sql-connector against HiveServer2 Thrift."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
HOST = "http://127.0.0.1:18455"
AGENT = "http://127.0.0.1:18102"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-sql"]


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
    env["DATABRICKS_ADDR"] = "127.0.0.1:18455"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_http(HOST + "/health")
    return proc


def main() -> int:
    from databricks import sql
    from databricks.sdk import WorkspaceClient

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-sql-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        w = WorkspaceClient(host=HOST, token=pat)
        wh = w.warehouses.create(name="e2e-thrift").result()
        if not wh.id:
            raise SystemExit("warehouse id missing")

        path = f"/sql/1.0/endpoints/{wh.id}"
        uri = f"http://127.0.0.1:18455{path}"
        conn = sql.connect(
            server_hostname="127.0.0.1",
            port=18455,
            http_path=path,
            access_token=pat,
            use_cloud_fetch=False,
            enable_telemetry=False,
            _connection_uri=uri,
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            rows = cur.fetchall()
            if not (len(rows) == 1 and len(rows[0]) == 1 and int(rows[0][0]) == 1):
                raise SystemExit(f"SELECT 1 returned {rows!r}")
            cur.close()
        finally:
            conn.close()

        try:
            bad = sql.connect(
                server_hostname="127.0.0.1",
                port=18455,
                http_path=path,
                access_token="dev",
                use_cloud_fetch=False,
                enable_telemetry=False,
                _connection_uri=uri,
            )
        except Exception:
            print("   token=dev refused")
        else:
            try:
                cur = bad.cursor()
                cur.execute("SELECT 1")
                cur.fetchall()
            except Exception:
                print("   token=dev refused")
                bad.close()
            else:
                bad.close()
                raise SystemExit("token=dev was accepted over Thrift")

        print("unmodified databricks-sql-connector 4.4.0 SELECT 1 over HiveServer2")
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
