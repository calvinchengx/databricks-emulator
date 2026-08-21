#!/usr/bin/env python3
"""Run dbt as a Databricks JOB -- a dbt_task -- and confirm the models with delta-rs.

This is the path e2e/dbt does not cover. There, dbt runs on this host and dials
the warehouse itself. Here nothing on this host runs dbt at all: a job is
created with a dbt_task, the emulator ships the project to the statement agent,
and dbt runs INSIDE that container against the warehouse it was told to use.

Five things make the witness hold.

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

4. THE CLIENT'S ADDRESS AND THE AGENT'S ARE DIFFERENT STRINGS, and the suite
   is arranged so that confusing them FAILS. `DATABRICKS_PUBLIC_URL` is the
   loopback address only this host can reach; `DATABRICKS_AGENT_URL` is the
   host-gateway name only the container can. Both were set to the agent-facing
   one before, which made them interchangeable here and nowhere else -- so the
   defect that shipped in #71, a profile carrying the CLIENT origin, was
   invisible to this gate and broke every dbt_task on a real compose stack.
   With them split, a profile built from the wrong one dials 127.0.0.1 inside
   the agent, reaches the agent itself, and the run fails.

5. A FAILING dbt RUN IS EXERCISED, not just a passing one. The second job runs
   a test that must fail, and requires the failure to arrive AS DATA carrying
   run_results.json. That is the other half of #71: a dbt failure used to leave
   the generated code as a SystemExit, which this agent answers by closing the
   connection without replying, so the caller saw a transport EOF and the
   artefacts were lost. Nothing on the success path can show that.

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
    # The two origins are DELIBERATELY different, and only one of them works
    # from inside the agent. PUBLIC_URL is what a client on this host is told;
    # 127.0.0.1 there means the agent's own container, where nothing listens on
    # this port. AGENT_URL is what a generated dbt profile has to carry. Setting
    # both to the agent-facing name -- which this suite used to do -- makes the
    # two indistinguishable and lets a profile built from the wrong one pass.
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_AGENT_URL"] = AGENT_FACING
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


def job_args(warehouse_id: str, commands: list[str]) -> dict:
    """One job body, so the passing and failing runs differ only in `commands`."""
    return {
        "name": "e2e-dbt-task",
        "tasks": [
            {
                "task_key": "gold",
                "dbt_task": {
                    "commands": commands,
                    "project_directory": WORKSPACE_DIR,
                    "warehouse_id": warehouse_id,
                    "catalog": "hive_metastore",
                    "schema": "default",
                },
            }
        ],
    }


def failing_run(token: str, body: dict) -> None:
    """A dbt test that fails must come back as DATA, carrying run_results.json.

    This is the case #71 fixed and no gate could see. The generated code used to
    surface a dbt failure by raising SystemExit; the statement agent answers
    that by closing the connection WITHOUT REPLYING, so the emulator saw
    `Post /statements: EOF` and the artefacts -- already printed, already
    correct -- were lost with the response that would have carried them. The
    caller learned the transport had failed, on a run where dbt had simply
    reported a failing test.

    So three things are required here, and they are not the same requirement:
    the run FAILS, the reason is dbt's own and not a transport error, and
    run_results.json survives and names the test. A run can fail for the wrong
    reason and satisfy only the first.
    """
    job = api("POST", "/api/2.2/jobs/create", token, body)
    run = api("POST", "/api/2.2/jobs/run-now", token, {"job_id": job["job_id"]})
    got = wait_run(int(run["run_id"]), token)
    state = got.get("state", {})
    out = api("GET", f"/api/2.2/jobs/runs/get-output?run_id={run['run_id']}", token)
    error = out.get("error", "")

    if state.get("result_state") != "FAILED":
        raise SystemExit(
            f"a failing dbt test did not fail the run: {state}\n"
            f"stdout: {out.get('logs', '')[:2000]}"
        )
    # The transport signature. `EOF` here means the agent closed the connection
    # instead of replying, which is the exact regression, and it arrives as a
    # FAILED run too -- so asserting FAILED alone would pass on it.
    for transport in ("EOF", "connection refused", "spark agent: status"):
        if transport in error:
            raise SystemExit(
                f"the run failed at the TRANSPORT, not in dbt: {error[:2000]}\n"
                "the failure has to travel as a field, or the artefacts go with it"
            )
    if "dbt test" not in error:
        raise SystemExit(f"the failure does not name the dbt command that failed: {error[:2000]}")

    artifacts = (out.get("dbt_output") or {}).get("artifacts") or {}
    if "run_results.json" not in artifacts:
        raise SystemExit(
            f"the run failed and run_results.json did not survive it: "
            f"{sorted(artifacts)} -- this is exactly what the caller needs on a "
            f"failing test, and exactly what the old SystemExit path lost"
        )
    results = json.loads(artifacts["run_results.json"]).get("results", [])
    failed = [r for r in results if r.get("status") in ("fail", "error")]
    if not failed:
        raise SystemExit(
            f"run_results.json came back but reports nothing failing: "
            f"{[(r.get('unique_id'), r.get('status')) for r in results]}"
        )
    named = [r for r in failed if "task_one_must_be_empty" in (r.get("unique_id") or "")]
    if not named:
        raise SystemExit(
            f"run_results.json does not name the test the fixture makes fail: "
            f"{[(r.get('unique_id'), r.get('status')) for r in results]}"
        )
    # `fail` is a test that ran and returned rows; `error` is one that could not
    # run at all. Both keep the artefact, which is what #71 is about, but only
    # `fail` means the warehouse actually executed the test -- so an `error`
    # here is reported rather than accepted, since it would mean the suite had
    # stopped exercising the SQL path it claims to.
    status = named[0].get("status")
    if status != "fail":
        raise SystemExit(
            f"the test came back as {status!r}, not 'fail': the artefact survived "
            f"but the test never ran against the warehouse. message: "
            f"{named[0].get('message')!r}"
        )
    print(
        f"   the failing run kept its evidence: {named[0]['unique_id']} "
        f"status={status} failures={named[0].get('failures')}"
    )


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
            # Whole command lines, the way Databricks documents them. Two of
            # them, so the run has to carry order.
            job_args(wid, ["dbt run --select task_one", "dbt run --select task_two"]),
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

        failing_run(pat, job_args(wid, ["dbt run --select task_one", "dbt test"]))

        print(
            "dbt ran inside the spark agent as a Jobs dbt_task; delta-rs confirmed the "
            "rows, and a failing dbt test came back as data with run_results.json"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
