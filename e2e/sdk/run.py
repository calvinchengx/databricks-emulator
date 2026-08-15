#!/usr/bin/env python3
"""Drive unmodified databricks-sdk against a freshly built emulator."""

from __future__ import annotations

import base64
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def wait_health(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url + "/health", timeout=0.5)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    raise SystemExit(f"emulator never became healthy at {url}")


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import DatabricksError
    from databricks.sdk.service.workspace import ImportFormat

    data = Path(tempfile.mkdtemp(prefix="dbx-e2e-"))
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18447"
    env["DATABRICKS_PUBLIC_URL"] = "http://127.0.0.1:18447"
    bin_path = data / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT, env=env)
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        wait_health("http://127.0.0.1:18447")
        pat = (data / "admin.pat").read_text().strip()
        w = WorkspaceClient(host="http://127.0.0.1:18447", token=pat)
        me = w.current_user.me()
        if me.user_name != "admin":
            raise SystemExit(f"me.user_name={me.user_name}")

        try:
            WorkspaceClient(host="http://127.0.0.1:18447", token="dev").current_user.me()
        except DatabricksError:
            pass
        else:
            raise SystemExit("token=dev was accepted")

        w.workspace.mkdirs("/Shared")
        w.workspace.upload("/Shared/hello.py", b"print(1)\n", overwrite=True, format=ImportFormat.AUTO)
        exported = w.workspace.download("/Shared/hello.py").read()
        if b"print(1)" not in exported:
            raise SystemExit(f"workspace download {exported!r}")

        w.dbfs.put("/tmp/e2e.txt", contents=base64.b64encode(b"dbfs-bytes").decode(), overwrite=True)
        got = w.dbfs.read("/tmp/e2e.txt")
        data = base64.b64decode(got.data or "")
        if data != b"dbfs-bytes":
            raise SystemExit(f"dbfs read {got!r}")

        versions = w.clusters.spark_versions()
        if not versions.versions or versions.versions[0].key != "emulator-spark":
            raise SystemExit(f"spark versions {versions!r}")
        try:
            w.clusters.create(
                cluster_name="e2e",
                spark_version="emulator-spark",
                node_type_id="emulator.session",
                num_workers=0,
            )
        except DatabricksError as exc:
            if "DATABRICKS_SPARK_CONNECT_URL" not in str(exc):
                raise SystemExit(f"cluster create without engine: {exc}")
        else:
            raise SystemExit("cluster create succeeded without an engine")

        print("e2e/sdk: identity + workspace + dbfs + cluster-refuse ok")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
