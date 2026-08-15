#!/usr/bin/env python3
"""Drive unmodified databricks-sdk Unity Catalog APIs through a UC OSS sidecar."""

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
HOST = "http://127.0.0.1:18450"
UC = "http://127.0.0.1:18080"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-uc"]


def wait_http(url: str, timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError, urllib.error.HTTPError) as exc:
            last = exc
            time.sleep(0.3)
    raise SystemExit(f"never healthy at {url}: {last}")


def start_emulator(bin_path: Path, data_dir: Path) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data_dir)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18450"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_UC_URL"] = UC
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


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import DatabricksError
    from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName, DataSourceFormat, TableType

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-uc-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True)
        wait_http(UC + "/api/2.1/unity-catalog/catalogs")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        w = WorkspaceClient(host=HOST, token=pat)

        cat = w.catalogs.create(name="e2e")
        if cat.name != "e2e":
            raise SystemExit(f"catalog create {cat}")
        sch = w.schemas.create(name="s", catalog_name="e2e")
        if sch.name != "s":
            raise SystemExit(f"schema create {sch}")
        tbl = w.tables.create(
            name="t",
            catalog_name="e2e",
            schema_name="s",
            table_type=TableType.EXTERNAL,
            data_source_format=DataSourceFormat.DELTA,
            storage_location="file:///tmp/e2e-uc-t",
            # UC OSS v0.5.0 stores Spark StructField JSON, not a bare datatype.
            columns=[ColumnInfo(
                name="id",
                type_name=ColumnTypeName.INT,
                type_text="int",
                type_json='{"name":"id","type":"integer","nullable":true,"metadata":{}}',
                position=0,
                nullable=True,
            )],
        )
        if tbl.name != "t" or tbl.table_type != TableType.EXTERNAL:
            raise SystemExit(f"table create {tbl}")
        got = w.tables.get("e2e.s.t")
        if got.name != "t" or got.table_type != TableType.EXTERNAL:
            raise SystemExit(f"table get {got}")
        names = [c.name for c in w.catalogs.list()]
        if "e2e" not in names:
            raise SystemExit(f"catalog list {names}")

        try:
            w.tables.create(
                name="m",
                catalog_name="e2e",
                schema_name="s",
                table_type=TableType.MANAGED,
                data_source_format=DataSourceFormat.DELTA,
                storage_location="file:///tmp/e2e-uc-m",
            )
            raise SystemExit("MANAGED table create must be refused")
        except DatabricksError as exc:
            if "MANAGED" not in str(exc) and "501" not in str(exc):
                raise SystemExit(f"managed refuse: {exc}")

        try:
            w.grants.get(securable_type="catalog", full_name="e2e")
            raise SystemExit("grants get must be refused")
        except DatabricksError as exc:
            if "grant" not in str(exc).lower() and "501" not in str(exc):
                raise SystemExit(f"grants refuse: {exc}")

        print("e2e/uc: catalog + schema + EXTERNAL table + managed 501 + grants 501 ok")
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False)


if __name__ == "__main__":
    sys.exit(main())
