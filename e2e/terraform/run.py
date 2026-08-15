#!/usr/bin/env python3
"""Drive unmodified databricks/databricks against a freshly built emulator."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
HOST = "http://127.0.0.1:18448"


def wait_health(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url + "/health", timeout=0.5)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    raise SystemExit(f"emulator never became healthy at {url}")


def terraform(args: list[str], env: dict[str, str], cwd: Path) -> subprocess.CompletedProcess[str]:
    cmd = ["terraform", f"-chdir={cwd}", *args]
    print("--", " ".join(cmd), flush=True)
    return subprocess.run(cmd, env=env, text=True, capture_output=False)


def main() -> int:
    if shutil.which("terraform") is None:
        raise SystemExit("terraform is required on PATH")

    data = Path(tempfile.mkdtemp(prefix="dbx-tf-"))
    work = data / "tf"
    shutil.copytree(HERE, work, ignore=shutil.ignore_patterns(".terraform", "*.tfstate*"))
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18448"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["TF_IN_AUTOMATION"] = "1"
    env["TF_INPUT"] = "0"
    bin_path = data / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT, env=env)
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        wait_health(HOST)
        pat = (data / "admin.pat").read_text().strip()
        try:
            urllib.request.urlopen(urllib.request.Request(
                HOST + "/api/2.0/preview/scim/v2/Me",
                headers={"Authorization": "Bearer dev"},
            ))
        except urllib.error.HTTPError as exc:
            if exc.code != 401:
                raise SystemExit(f"token=dev → {exc.code}")
        else:
            raise SystemExit("token=dev was accepted")
        tfenv = env.copy()
        tfenv["DATABRICKS_HOST"] = HOST
        tfenv["DATABRICKS_TOKEN"] = pat
        tfenv["DATABRICKS_AUTH_TYPE"] = "pat"

        if terraform(["init", "-input=false"], tfenv, work).returncode != 0:
            raise SystemExit("terraform init failed")
        if terraform(["apply", "-auto-approve", "-input=false"], tfenv, work).returncode != 0:
            raise SystemExit("terraform apply failed")

        out = subprocess.check_output(["terraform", f"-chdir={work}", "output", "-json"], env=tfenv, text=True)
        values = {k: v.get("value") for k, v in json.loads(out).items()}
        if values.get("user_home") != "/Users/admin":
            raise SystemExit(f"user_home {values!r}")
        if values.get("notebook_path") != "/Users/admin/tf-hello":
            raise SystemExit(f"notebook_path {values!r}")
        if values.get("workspace_file_path") != "/Users/admin/tf-job.py":
            raise SystemExit(f"workspace_file_path {values!r}")
        if not values.get("job_id"):
            raise SystemExit(f"job_id {values!r}")

        req = urllib.request.Request(
            HOST + "/api/2.0/workspace/export?path=/Users/admin/tf-hello&direct_download=true",
            headers={"Authorization": "Bearer " + pat},
        )
        with urllib.request.urlopen(req) as resp:
            exported = resp.read()
        if b"tf-hello" not in exported:
            raise SystemExit(f"notebook export {exported!r}")

        req = urllib.request.Request(
            HOST + "/api/2.0/workspace-files/Users/admin/tf-job.py",
            headers={"Authorization": "Bearer " + pat},
        )
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
        if raw != b'print("tf-job")\n' and raw != b'print("tf-job")':
            raise SystemExit(f"workspace-file bytes {raw!r}")

        if terraform(["destroy", "-auto-approve", "-input=false"], tfenv, work).returncode != 0:
            raise SystemExit("terraform destroy failed")

        bad = tfenv.copy()
        bad["DATABRICKS_TOKEN"] = "dev"
        plan = terraform(["plan", "-input=false", "-refresh=false"], bad, work)
        if plan.returncode == 0:
            raise SystemExit("token=dev was accepted by terraform plan")

        print("e2e/terraform: current_user + notebook + workspace_file + job + token=dev refused")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
