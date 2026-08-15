#!/usr/bin/env python3
"""Drive the unmodified Databricks CLI against a freshly built emulator.

The CLI is a different witness from the SDK even though both speak the same
REST. It ships its own auth resolution, its own request shapes, and its own
opinion about what a well-formed response looks like — so a surface the SDK
is happy with can still fail here. Nothing below is our client: every
assertion reads what the real `databricks` binary printed.
"""

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
HOST = "http://127.0.0.1:18454"


def wait_health(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url + "/health", timeout=0.5)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    raise SystemExit(f"emulator never became healthy at {url}")


def stop(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def cli(args: list[str], env: dict[str, str], check: bool = True) -> str:
    """Run the real CLI. stdout is returned; stderr stays on the console."""
    cmd = ["databricks", *args]
    print("--", " ".join(cmd), flush=True)
    proc = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr, flush=True)
    if check and proc.returncode != 0:
        raise SystemExit(f"databricks {' '.join(args)} exited {proc.returncode}")
    return proc.stdout


def cli_json(args: list[str], env: dict[str, str]):
    raw = cli([*args, "--output", "json"], env)
    if not raw.strip():
        raise SystemExit(f"databricks {' '.join(args)} printed nothing")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"databricks {' '.join(args)} printed non-JSON: {exc}\n{raw[:400]}")


def main() -> int:
    if shutil.which("databricks") is None:
        raise SystemExit("the databricks CLI is required on PATH (https://docs.databricks.com/dev-tools/cli/)")

    data = Path(tempfile.mkdtemp(prefix="dbx-cli-"))
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18454"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    bin_path = data / "databricks-emulator"
    subprocess.check_call(
        ["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT, env=env
    )
    proc = None
    try:
        proc = subprocess.Popen(
            [str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        wait_health(HOST)
        pat = (data / "admin.pat").read_text().strip()

        # The CLI resolves auth itself from these three; no profile file, no
        # ~/.databrickscfg, nothing of ours in the request path.
        cenv = env.copy()
        cenv["DATABRICKS_HOST"] = HOST
        cenv["DATABRICKS_TOKEN"] = pat
        cenv["DATABRICKS_AUTH_TYPE"] = "pat"
        cenv.pop("DATABRICKS_CONFIG_FILE", None)

        print(cli(["--version"], cenv).strip())

        # Identity. The CLI's own auth handshake, not a hand-built header.
        me = cli_json(["current-user", "me"], cenv)
        if not me.get("userName"):
            raise SystemExit(f"current-user me returned no userName: {me}")
        print(f"   current-user me -> {me['userName']}")

        # A bad token must be refused through the same door.
        bad = cenv.copy()
        bad["DATABRICKS_TOKEN"] = "dev"
        if subprocess.run(
            ["databricks", "current-user", "me"], env=bad, text=True, capture_output=True
        ).returncode == 0:
            raise SystemExit("token=dev was accepted by the CLI")
        print("   token=dev refused")

        # Workspace: import a notebook, list it, export it back.
        local = data / "hello.py"
        local.write_text("# Databricks notebook source\nprint('hello from the CLI')\n")
        cli(
            ["workspace", "import", "/cli-witness/hello",
             "--file", str(local), "--language", "PYTHON", "--format", "SOURCE"],
            cenv,
        )
        listing = cli_json(["workspace", "list", "/cli-witness"], cenv)
        paths = [obj.get("path") for obj in listing] if isinstance(listing, list) else []
        if "/cli-witness/hello" not in paths:
            raise SystemExit(f"workspace list did not show the imported notebook: {listing}")
        exported = cli(["workspace", "export", "/cli-witness/hello", "--format", "SOURCE"], cenv)
        if "hello from the CLI" not in exported:
            raise SystemExit(f"workspace export lost the body: {exported[:200]}")
        print("   workspace import/list/export round-tripped")

        # DBFS through the CLI's own fs commands.
        payload = data / "payload.txt"
        payload.write_text("dbfs via the CLI\n")
        cli(["fs", "mkdir", "dbfs:/cli-witness"], cenv)
        cli(["fs", "cp", str(payload), "dbfs:/cli-witness/payload.txt", "--overwrite"], cenv)
        cat = cli(["fs", "cat", "dbfs:/cli-witness/payload.txt"], cenv)
        if "dbfs via the CLI" not in cat:
            raise SystemExit(f"fs cat did not return what fs cp wrote: {cat[:200]}")
        entries = cli_json(["fs", "ls", "dbfs:/cli-witness"], cenv)
        names = [e.get("name") or e.get("path") for e in entries] if isinstance(entries, list) else []
        if not any("payload.txt" in str(n) for n in names):
            raise SystemExit(f"fs ls did not show the uploaded file: {entries}")
        print("   fs mkdir/cp/cat/ls round-tripped")

        # Secrets: create a scope, put a secret, and confirm the value is
        # never handed back — the refusal is as much the witness as the write.
        cli(["secrets", "create-scope", "cli-witness"], cenv)
        cli(["secrets", "put-secret", "cli-witness", "token", "--string-value", "s3cret"], cenv)
        scopes = cli_json(["secrets", "list-scopes"], cenv)
        scope_names = [s.get("name") for s in scopes] if isinstance(scopes, list) else []
        if "cli-witness" not in scope_names:
            raise SystemExit(f"list-scopes missing the created scope: {scopes}")
        keys = cli_json(["secrets", "list-secrets", "cli-witness"], cenv)
        key_names = [k.get("key") for k in keys] if isinstance(keys, list) else []
        if "token" not in key_names:
            raise SystemExit(f"list-secrets missing the put key: {keys}")
        if "s3cret" in json.dumps(keys):
            raise SystemExit("list-secrets leaked the secret value")
        print("   secrets scope/put/list, value withheld")

        # Tokens: the CLI mints a PAT through the same identity surface.
        created = cli_json(["tokens", "create", "--comment", "cli-witness"], cenv)
        if not created.get("token_value"):
            raise SystemExit(f"tokens create returned no token_value: {created}")
        print("   tokens create returned a usable PAT")

        # SQL warehouses: create, then see it in the CLI's own list.
        wh = cli_json(["warehouses", "create", "--name", "cli-witness"], cenv)
        wh_id = wh.get("id")
        if not wh_id:
            raise SystemExit(f"warehouses create returned no id: {wh}")
        listed = cli_json(["warehouses", "list"], cenv)
        ids = [w.get("id") for w in listed] if isinstance(listed, list) else []
        if wh_id not in ids:
            raise SystemExit(f"warehouses list did not show {wh_id}: {listed}")
        print("   warehouses create/list")

        # Clusters: the node-type and version catalogues the CLI reads.
        versions = cli_json(["clusters", "spark-versions"], cenv)
        if not versions:
            raise SystemExit("clusters spark-versions returned nothing")
        print("   clusters spark-versions")

        # Jobs: create through the CLI's JSON input, then find it in list.
        job_spec = data / "job.json"
        job_spec.write_text(json.dumps({
            "name": "cli-witness",
            "tasks": [{
                "task_key": "only",
                "notebook_task": {"notebook_path": "/cli-witness/hello"},
            }],
        }))
        job = cli_json(["jobs", "create", "--json", f"@{job_spec}"], cenv)
        job_id = job.get("job_id")
        if not job_id:
            raise SystemExit(f"jobs create returned no job_id: {job}")
        jobs = cli_json(["jobs", "list"], cenv)
        job_ids = [j.get("job_id") for j in jobs] if isinstance(jobs, list) else []
        if job_id not in job_ids:
            raise SystemExit(f"jobs list did not show {job_id}: {jobs}")
        print("   jobs create/list")

        print("\nunmodified databricks CLI drove every surface above")
        return 0
    finally:
        stop(proc)
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
