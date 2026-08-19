#!/usr/bin/env python3
"""Run dbt as a Databricks JOB -- a dbt_task -- and confirm the models with delta-rs.

This is the path e2e/dbt does not cover. There, dbt runs on this host and dials
the warehouse itself. Here nothing on this host runs dbt at all: a job is
created with a dbt_task, the emulator ships the project to the statement agent,
and dbt runs INSIDE that container against the warehouse it was told to use.

Three things make the witness hold.

1. THE HOST HAS NO DBT. This suite runs under the `delta` dependency group,
   which carries deltalake and no dbt of any kind. `dbt_is_absent_here` asserts
   that rather than trusting it, so "dbt ran" cannot quietly mean "dbt ran
   here". The only dbt in the picture is the one baked into the agent image.

2. THE RUN'S OWN VERDICT IS NOT THE EVIDENCE. A job reporting SUCCESS proves
   the emulator is content; it does not prove any SQL executed. The models
   publish external Delta copies onto a volume shared with Sail, and delta-rs
   -- which never spoke to dbt, the agent, or Sail -- reads the log and the
   rows back. The engine that wrote is not the one that confirms.

3. ref() IS EXERCISED. task_two selects from task_one, so a dbt that resolved
   the DAG but never ran it, or ran the models in the wrong order, produces a
   missing table rather than a passing run.

The agent reaches the emulator over host.docker.internal, which is why the
emulator binds 0.0.0.0 here rather than loopback: on real Databricks the
compute dials the warehouse over the network, and a loopback-only listener is
an accident of running both halves on one machine.
"""

from __future__ import annotations

import base64
import json
import os
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
PROJECT = HERE / "project"
PORT = 18458
HOST = f"http://127.0.0.1:{PORT}"
# What the AGENT dials. Not the same string as HOST: this one has to resolve
# from inside a container, and 127.0.0.1 there is the container itself.
AGENT_FACING = f"http://host.docker.internal:{PORT}"
AGENT = "http://127.0.0.1:18105"
WORKSPACE_DIR = "/Workspace/dbt/emulator_dbt_task"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-dbt-task"]


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
    # 0.0.0.0, not loopback: the agent is in a container and arrives on the
    # host-gateway address, which a 127.0.0.1 listener refuses.
    env["DATABRICKS_ADDR"] = f"0.0.0.0:{PORT}"
    env["DATABRICKS_PUBLIC_URL"] = AGENT_FACING
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    proc = subprocess.Popen(
        [str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
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


def dbt_is_absent_here() -> None:
    """The host must not be able to run dbt, or 'dbt ran' proves nothing."""
    try:
        import dbt.cli.main  # noqa: F401
    except ImportError:
        return
    raise SystemExit(
        "dbt is importable on this host, so a passing run would not show that "
        "dbt ran in the AGENT. Run this suite under the `delta` group, which "
        "carries no dbt."
    )


def upload_project(token: str) -> list[str]:
    """Put the dbt project into the WORKSPACE, which is where dbt_task reads it.

    format=RAW, because these are workspace files and not notebooks. A dbt
    project imported as SOURCE would be stored as Python and come back wrong.
    """
    sent = []
    for path in sorted(PROJECT.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(PROJECT).as_posix()
        api(
            "POST",
            "/api/2.0/workspace/import",
            token,
            {
                "path": f"{WORKSPACE_DIR}/{rel}",
                "format": "RAW",
                "overwrite": True,
                "content": base64.b64encode(path.read_bytes()).decode(),
            },
        )
        sent.append(rel)
    if "dbt_project.yml" not in sent:
        raise SystemExit(f"fixture has no dbt_project.yml: {sent}")
    return sent


def wait_run(run_id: int, token: str, timeout: float = 300.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = api("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}", token)
        state = last.get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return last
        time.sleep(1.0)
    raise SystemExit(f"run {run_id} never reached a terminal state: {last}")


def confirm(table_dir: Path, want_rows: list[tuple[int]], model: str) -> int:
    """Read a materialized model with delta-rs. Neither dbt nor the run is consulted."""
    from deltalake import DeltaTable

    log = table_dir / "_delta_log"
    if not log.is_dir():
        listing = sorted(p.name for p in table_dir.rglob("*")) if table_dir.exists() else []
        raise SystemExit(
            f"the run reported SUCCESS but {model} has no _delta_log at {table_dir}: {listing[:20]}"
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
    dbt_is_absent_here()

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-dbt-task-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(
        ["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT
    )
    host_root = Path(tempfile.mkdtemp(prefix="dbx-dbt-task-tables-"))
    os.chmod(host_root, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    env = os.environ.copy()
    env["DBT_DATA"] = str(host_root)

    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()

        wh = api("POST", "/api/2.0/sql/warehouses", pat, {"name": "e2e-dbt-task"})
        wid = wh.get("id")
        if not wid:
            raise SystemExit(f"warehouse id missing: {wh!r}")

        sent = upload_project(pat)
        print(f"   project in the workspace: {', '.join(sent)}")

        job = api(
            "POST",
            "/api/2.2/jobs/create",
            pat,
            {
                "name": "e2e-dbt-task",
                "tasks": [
                    {
                        "task_key": "gold",
                        "dbt_task": {
                            # Whole command lines, the way Databricks documents
                            # them. Two of them, so the run has to carry order.
                            "commands": [
                                "dbt run --select task_one",
                                "dbt run --select task_two",
                            ],
                            "project_directory": WORKSPACE_DIR,
                            "warehouse_id": wid,
                            "catalog": "hive_metastore",
                            "schema": "default",
                        },
                    }
                ],
            },
        )
        job_id = job.get("job_id")
        if not job_id:
            raise SystemExit(f"job_id missing: {job!r}")

        run = api("POST", "/api/2.2/jobs/run-now", pat, {"job_id": job_id})
        run_id = run.get("run_id")
        if not run_id:
            raise SystemExit(f"run_id missing: {run!r}")

        got = wait_run(int(run_id), pat)
        state = got.get("state", {})
        if state.get("result_state") != "SUCCESS":
            out = api("GET", f"/api/2.2/jobs/runs/get-output?run_id={run_id}", pat)
            raise SystemExit(
                f"dbt_task did not succeed: {state}\n"
                f"stdout: {out.get('logs', '')[:2000]}\n"
                f"error:  {out.get('error', '')[:2000]}"
            )

        # The witness. delta-rs never spoke to dbt, the agent or Sail.
        v1 = confirm(host_root / "task_one", [(1,)], "task_one")
        v2 = confirm(host_root / "task_two", [(1,)], "task_two")
        print(f"   delta-rs confirmed task_one (v{v1}) and task_two (v{v2}) via ref()")

        # The run's own output should name dbt, so a SUCCESS that skipped the
        # invocation entirely is not mistaken for a working task.
        out = api("GET", f"/api/2.2/jobs/runs/get-output?run_id={run_id}", pat)
        logs = out.get("logs", "")
        if "dbt ok:" not in logs:
            raise SystemExit(
                f"the run succeeded but its output does not show dbt running: {logs[:2000]}"
            )

        print(
            "dbt ran inside the spark agent as a Jobs dbt_task; delta-rs confirmed the rows"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
