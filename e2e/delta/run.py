#!/usr/bin/env python3
"""Sail writes Delta through the warehouse API; delta-rs confirms the log.

The engine that wrote is never the one that confirms. A warehouse COUNT(*)
after INSERT is not a witness.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
HOST = "http://127.0.0.1:18451"
AGENT = "http://127.0.0.1:18100"
TABLE = "file:///data/delta/e2e/events"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-delta"]


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


def start_emulator(bin_path: Path, data_dir: Path) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data_dir)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18451"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_http(HOST + "/health")
    return proc


def stop(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def sql(w, warehouse_id: str, statement: str) -> None:
    stmt = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement)
    state = stmt.status.state.value if stmt.status and stmt.status.state else None
    if state not in {"SUCCEEDED", "SUCCESS"}:
        err = ""
        if stmt.status and getattr(stmt.status, "error", None) is not None:
            err = str(stmt.status.error)
        raise SystemExit(f"sql {statement!r}: state={state} {err or stmt}")


def confirm(host_table: Path, want_rows: list[tuple[int, str]], min_version: int) -> int:
    """Read with delta-rs. Sail must not be imported here."""
    if "pyspark" in sys.modules or "sail" in sys.modules:
        raise SystemExit("confirmer must not import the writer")
    from deltalake import DeltaTable

    log = host_table / "_delta_log"
    if not log.is_dir():
        listing = list(host_table.rglob("*")) if host_table.exists() else []
        raise SystemExit(
            f"Sail reported success but no _delta_log at {host_table}: {listing[:20]}"
        )
    dt = DeltaTable(str(host_table))
    version = dt.version()
    if version < min_version:
        raise SystemExit(f"delta log version {version} < {min_version}")
    pdf = dt.to_pandas()
    cols = {c.lower(): c for c in pdf.columns}
    if "id" not in cols or "name" not in cols:
        raise SystemExit(f"delta-rs columns {list(pdf.columns)} missing id/name")
    got = sorted((int(row[0]), str(row[1])) for row in pdf[[cols["id"], cols["name"]]].itertuples(index=False, name=None))
    want = sorted(want_rows)
    if got != want:
        raise SystemExit(f"delta-rs rows {got} != {want}")
    return version


def main() -> int:
    from databricks.sdk import WorkspaceClient

    host_root = Path(tempfile.mkdtemp(prefix="dbx-delta-"))
    host_table = host_root / "e2e" / "events"
    host_table.parent.mkdir(parents=True)
    for p in (host_root, host_table.parent):
        os.chmod(p, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-delta-emu-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    proc = None
    env = os.environ.copy()
    env["DELTA_DATA"] = str(host_root)
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        w = WorkspaceClient(host=HOST, token=pat)
        wh = w.warehouses.create(name="e2e-delta").result()
        if wh.id is None:
            raise SystemExit("warehouse id missing")

        sql(w, wh.id, f"CREATE TABLE events (id INT, name STRING) USING delta LOCATION '{TABLE}'")
        sql(w, wh.id, "INSERT INTO events VALUES (1, 'alice'), (2, 'bob')")
        v1 = confirm(host_table, [(1, "alice"), (2, "bob")], min_version=0)

        sql(w, wh.id, "INSERT INTO events VALUES (3, 'carol')")
        v2 = confirm(host_table, [(1, "alice"), (2, "bob"), (3, "carol")], min_version=v1 + 1)
        if v2 <= v1:
            raise SystemExit(f"second insert did not advance the log: {v1} -> {v2}")

        print(f"e2e/delta: Sail wrote, delta-rs confirmed versions {v1} then {v2}")
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    sys.exit(main())
