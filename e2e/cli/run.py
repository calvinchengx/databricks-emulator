#!/usr/bin/env python3
"""Drive the unmodified Databricks CLI against a freshly built emulator.

The CLI is a different witness from the SDK even though both speak the same
REST. It ships its own auth resolution, its own request shapes, and its own
opinion about what a well-formed response looks like — so a surface the SDK
is happy with can still fail here. Nothing below is our client: every
assertion reads what the real `databricks` binary printed.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HOST = "http://127.0.0.1:18454"

# https://github.com/databricks/cli/releases/tag/v1.12.1
CLI_VERSION = "1.12.1"
CLI_ASSETS: dict[tuple[str, str], tuple[str, str]] = {
    ("linux", "x86_64"): (
        "databricks_cli_1.12.1_linux_amd64.zip",
        "60833feee22caa98f575c4948ccbbc9473fda0dd80c9b79544dc7b079f6a9815",
    ),
    ("linux", "amd64"): (
        "databricks_cli_1.12.1_linux_amd64.zip",
        "60833feee22caa98f575c4948ccbbc9473fda0dd80c9b79544dc7b079f6a9815",
    ),
    ("linux", "aarch64"): (
        "databricks_cli_1.12.1_linux_arm64.zip",
        "8c2a3acd5f563fd8e3c9e64ee4566e1ec7cef087934fd4fb099d92b709f7e3a2",
    ),
    ("linux", "arm64"): (
        "databricks_cli_1.12.1_linux_arm64.zip",
        "8c2a3acd5f563fd8e3c9e64ee4566e1ec7cef087934fd4fb099d92b709f7e3a2",
    ),
    ("darwin", "x86_64"): (
        "databricks_cli_1.12.1_darwin_amd64.zip",
        "5c609f7743941150138d15cc1891a0a32b643eb4b66f46701005026e944b2aad",
    ),
    ("darwin", "amd64"): (
        "databricks_cli_1.12.1_darwin_amd64.zip",
        "5c609f7743941150138d15cc1891a0a32b643eb4b66f46701005026e944b2aad",
    ),
    ("darwin", "arm64"): (
        "databricks_cli_1.12.1_darwin_arm64.zip",
        "b43e40d2ea7046866849755747a8cb7b4a322e694c0215e53b481eb842bc908c",
    ),
}


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


def fetch_cli(dest: Path) -> Path:
    system = platform.system().lower()
    machine = platform.machine().lower()
    asset = CLI_ASSETS.get((system, machine))
    if asset is None:
        raise SystemExit(f"no pinned CLI build for {system}/{machine}")
    name, digest = asset
    dest.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/databricks/cli/releases/download/v{CLI_VERSION}/{name}"
    archive = dest / name
    print(f"-- fetch {url}", flush=True)
    urllib.request.urlretrieve(url, archive)
    got = hashlib.sha256(archive.read_bytes()).hexdigest()
    if got != digest:
        raise SystemExit(f"CLI sha256 {got} != {digest}")
    with zipfile.ZipFile(archive) as zf:
        zf.extractall(dest)
    binary = dest / ("databricks.exe" if system == "windows" else "databricks")
    if not binary.is_file():
        raise SystemExit(f"CLI binary missing after extract: {list(dest.rglob('*'))}")
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    return binary


def cli(bin_path: Path, args: list[str], env: dict[str, str], check: bool = True) -> str:
    """Run the pinned CLI. stdout is returned; stderr stays on the console."""
    cmd = [str(bin_path), *args]
    print("--", " ".join(cmd), flush=True)
    proc = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr, flush=True)
    if check and proc.returncode != 0:
        raise SystemExit(f"databricks {' '.join(args)} exited {proc.returncode}")
    return proc.stdout


def cli_json(bin_path: Path, args: list[str], env: dict[str, str]):
    raw = cli(bin_path, [*args, "--output", "json"], env)
    if not raw.strip():
        raise SystemExit(f"databricks {' '.join(args)} printed nothing")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"databricks {' '.join(args)} printed non-JSON: {exc}\n{raw[:400]}")


def start_emulator(bin_path: Path, env: dict[str, str]) -> subprocess.Popen[bytes]:
    proc = subprocess.Popen(
        [str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    wait_health(HOST)
    return proc


def main() -> int:
    data = Path(tempfile.mkdtemp(prefix="dbx-cli-"))
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18454"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    emu = data / "databricks-emulator"
    subprocess.check_call(
        ["go", "build", "-o", str(emu), "./cmd/databricks-emulator"], cwd=ROOT, env=env
    )
    dbx = fetch_cli(data / "cli")
    proc = None
    try:
        proc = start_emulator(emu, env)
        pat = (data / "admin.pat").read_text().strip()

        # The CLI resolves auth itself. An empty config file keeps
        # ~/.databrickscfg from leaking into the request path.
        cfg = data / "empty.cfg"
        cfg.write_text("")
        cenv = env.copy()
        cenv["DATABRICKS_HOST"] = HOST
        cenv["DATABRICKS_TOKEN"] = pat
        cenv["DATABRICKS_AUTH_TYPE"] = "pat"
        cenv["DATABRICKS_CONFIG_FILE"] = str(cfg)

        print(cli(dbx, ["--version"], cenv).strip())

        me = cli_json(dbx, ["current-user", "me"], cenv)
        if not me.get("userName"):
            raise SystemExit(f"current-user me returned no userName: {me}")
        print(f"   current-user me -> {me['userName']}")

        bad = cenv.copy()
        bad["DATABRICKS_TOKEN"] = "dev"
        denied = subprocess.run(
            [str(dbx), "current-user", "me"], env=bad, text=True, capture_output=True
        )
        if denied.returncode == 0:
            raise SystemExit("token=dev was accepted by the CLI")
        print("   token=dev refused")

        local = data / "hello.py"
        local.write_text("# Databricks notebook source\nprint('hello from the CLI')\n")
        cli(
            dbx,
            ["workspace", "import", "/cli-witness/hello",
             "--file", str(local), "--language", "PYTHON", "--format", "SOURCE"],
            cenv,
        )
        listing = cli_json(dbx, ["workspace", "list", "/cli-witness"], cenv)
        paths = [obj.get("path") for obj in listing] if isinstance(listing, list) else []
        if "/cli-witness/hello" not in paths:
            raise SystemExit(f"workspace list did not show the imported notebook: {listing}")
        exported = cli(dbx, ["workspace", "export", "/cli-witness/hello", "--format", "SOURCE"], cenv)
        if "hello from the CLI" not in exported:
            raise SystemExit(f"workspace export lost the body: {exported[:200]}")

        raw_src = data / "job.py"
        raw_src.write_text('print("cli-job")\n')
        cli(
            dbx,
            ["workspace", "import", "/cli-witness/job.py",
             "--file", str(raw_src), "--format", "RAW"],
            cenv,
        )
        raw = cli(dbx, ["workspace", "export", "/cli-witness/job.py", "--format", "RAW"], cenv)
        if "cli-job" not in raw:
            raise SystemExit(f"RAW export lost the body: {raw[:200]}")
        print("   workspace SOURCE + RAW import/export round-tripped")

        payload = data / "payload.txt"
        payload.write_text("dbfs via the CLI\n")
        cli(dbx, ["fs", "mkdir", "dbfs:/cli-witness"], cenv)
        cli(dbx, ["fs", "cp", str(payload), "dbfs:/cli-witness/payload.txt", "--overwrite"], cenv)
        cat = cli(dbx, ["fs", "cat", "dbfs:/cli-witness/payload.txt"], cenv)
        if "dbfs via the CLI" not in cat:
            raise SystemExit(f"fs cat did not return what fs cp wrote: {cat[:200]}")
        entries = cli_json(dbx, ["fs", "ls", "dbfs:/cli-witness"], cenv)
        names = [e.get("name") or e.get("path") for e in entries] if isinstance(entries, list) else []
        if not any("payload.txt" in str(n) for n in names):
            raise SystemExit(f"fs ls did not show the uploaded file: {entries}")
        print("   fs mkdir/cp/cat/ls round-tripped")

        cli(dbx, ["secrets", "create-scope", "cli-witness"], cenv)
        cli(dbx, ["secrets", "put-secret", "cli-witness", "token", "--string-value", "s3cret"], cenv)
        scopes = cli_json(dbx, ["secrets", "list-scopes"], cenv)
        scope_names = [s.get("name") for s in scopes] if isinstance(scopes, list) else []
        if "cli-witness" not in scope_names:
            raise SystemExit(f"list-scopes missing the created scope: {scopes}")
        keys = cli_json(dbx, ["secrets", "list-secrets", "cli-witness"], cenv)
        key_names = [k.get("key") for k in keys] if isinstance(keys, list) else []
        if "token" not in key_names:
            raise SystemExit(f"list-secrets missing the put key: {keys}")
        if "s3cret" in json.dumps(keys):
            raise SystemExit("list-secrets leaked the secret value")

        stop(proc)
        proc = start_emulator(emu, env)
        scopes = cli_json(dbx, ["secrets", "list-scopes"], cenv)
        scope_names = [s.get("name") for s in scopes] if isinstance(scopes, list) else []
        if "cli-witness" not in scope_names:
            raise SystemExit(f"scope did not survive restart: {scopes}")
        keys = cli_json(dbx, ["secrets", "list-secrets", "cli-witness"], cenv)
        if "token" not in [k.get("key") for k in keys]:
            raise SystemExit(f"secret key did not survive restart: {keys}")
        if "s3cret" in json.dumps(keys):
            raise SystemExit("list-secrets leaked the secret value after restart")
        print("   secrets persist across restart, value withheld")

        created = cli_json(dbx, ["tokens", "create", "--comment", "cli-witness"], cenv)
        if not created.get("token_value"):
            raise SystemExit(f"tokens create returned no token_value: {created}")
        print("   tokens create returned a usable PAT")

        wh = cli_json(dbx, ["warehouses", "create", "--name", "cli-witness"], cenv)
        wh_id = wh.get("id")
        if not wh_id:
            raise SystemExit(f"warehouses create returned no id: {wh}")
        listed = cli_json(dbx, ["warehouses", "list"], cenv)
        ids = [w.get("id") for w in listed] if isinstance(listed, list) else []
        if wh_id not in ids:
            raise SystemExit(f"warehouses list did not show {wh_id}: {listed}")
        print("   warehouses create/list")

        versions = cli_json(dbx, ["clusters", "spark-versions"], cenv)
        if not versions:
            raise SystemExit("clusters spark-versions returned nothing")
        print("   clusters spark-versions")

        job_spec = data / "job.json"
        job_spec.write_text(json.dumps({
            "name": "cli-witness",
            "tasks": [{
                "task_key": "only",
                "spark_python_task": {"python_file": "/cli-witness/job.py"},
            }],
        }))
        job = cli_json(dbx, ["jobs", "create", "--json", f"@{job_spec}"], cenv)
        job_id = job.get("job_id")
        if not job_id:
            raise SystemExit(f"jobs create returned no job_id: {job}")
        jobs = cli_json(dbx, ["jobs", "list"], cenv)
        job_ids = [j.get("job_id") for j in jobs] if isinstance(jobs, list) else []
        if job_id not in job_ids:
            raise SystemExit(f"jobs list did not show {job_id}: {jobs}")
        print("   jobs create/list")

        print("\nunmodified databricks CLI v1.12.1 drove every surface above")
        return 0
    finally:
        stop(proc)
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
