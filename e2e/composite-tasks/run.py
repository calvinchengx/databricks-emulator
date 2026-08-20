#!/usr/bin/env python3
"""Drive `for_each_task` and `run_job_task` with the unmodified Databricks SDK,
and let the filesystem say what actually ran.

The Go witnesses for this row assert what the fake Spark SAW — a real property,
but an internal one: our client, our engine stub, our assertions. This suite
replaces every part of that. The unmodified `databricks-sdk` builds the jobs
(`jobs.ForEachTask`, `jobs.RunJobTask`), a real Sail engine runs the tasks, and
**delta-rs** reads the result, so the evidence is a Delta table on disk rather
than the run record the emulator wrote about itself.

The two questions it answers, neither of which the API response can:

  * for_each: did EACH input run, with ITS OWN value? One table per input, named
    by the input, is the only way to tell three iterations of the same value
    from three iterations of different ones — and getting that wrong is silent.
  * run_job:  did the CHILD really run? The parent writes nothing itself, so a
    table exists only if the child's task reached the engine.
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
PORT = 18461
HOST = f"http://127.0.0.1:{PORT}"
AGENT = "http://127.0.0.1:18107"
WORKSPACE_DIR = "/Workspace/comp"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-comp"]


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
    env["DATABRICKS_ADDR"] = f"127.0.0.1:{PORT}"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    proc = subprocess.Popen(
        [str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    wait_http(HOST + "/health")
    return proc




def wait_run(w, run_id: int, timeout: float = 240.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = w.jobs.get_run(run_id=run_id)
        state = last.state
        if state and str(getattr(state.life_cycle_state, "value", state.life_cycle_state)) in (
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
        ):
            return last
        time.sleep(1.0)
    raise SystemExit(f"run {run_id} never terminated: {last}")



# One table per invocation, named by the value the task was given. A shared name
# would make three iterations indistinguishable from one, which is the failure
# this suite exists to detect.
TASK_SOURCE = """import sys
import pyarrow as pa
from deltalake import write_deltalake

name = sys.argv[1] if len(sys.argv) > 1 else "none"
write_deltalake("/data/comp/" + name, pa.table({"ran": [name]}))
print("task ran:", name)
"""

INPUTS = ["alpha", "beta", "gamma"]


def upload(token: str, path: str, source: str) -> None:
    payload = json.dumps({
        "path": path,
        "format": "SOURCE",
        "language": "PYTHON",
        "overwrite": True,
        "content": base64.b64encode(source.encode()).decode(),
    }).encode()
    req = urllib.request.Request(
        HOST + "/api/2.0/workspace/import",
        data=payload,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req).read()


def tables(root: Path) -> set[str]:
    return {p.name for p in root.iterdir() if (p / "_delta_log").is_dir()} if root.exists() else set()


def read_ran(root: Path, name: str) -> list[str]:
    from deltalake import DeltaTable

    return [str(v) for v in DeltaTable(str(root / name)).to_pyarrow_table().column("ran").to_pylist()]


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-comp-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    host_root = Path(tempfile.mkdtemp(prefix="dbx-comp-tables-"))
    os.chmod(host_root, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    env = os.environ.copy()
    env["COMP_DATA"] = str(host_root)

    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        upload(pat, f"{WORKSPACE_DIR}/task.py", TASK_SOURCE)
        w = WorkspaceClient(host=HOST, token=pat)

        # --- for_each_task -------------------------------------------------
        # Built through the SDK's own types, so a field it models differently
        # from what this emulator accepts fails here rather than in our tests.
        loop = w.jobs.create(
            name="e2e-comp-foreach",
            tasks=[jobs.Task(
                task_key="fan",
                for_each_task=jobs.ForEachTask(
                    inputs=json.dumps(INPUTS),
                    concurrency=1,  # serial: see the note in the module docstring
                    task=jobs.Task(
                        task_key="inner",
                        spark_python_task=jobs.SparkPythonTask(
                            python_file=f"{WORKSPACE_DIR}/task.py",
                            parameters=["{{input}}"],
                        ),
                    ),
                ),
            )],
        )
        run = w.jobs.run_now(job_id=loop.job_id)
        got = wait_run(w, int(run.run_id if hasattr(run, "run_id") else run.response.run_id))
        result = str(getattr(got.state.result_state, "value", got.state.result_state))
        if result != "SUCCESS":
            raise SystemExit(f"for_each run ended {result}: {got.state}")

        # delta-rs, not the run record: one table per input, each holding its own
        # value. Three tables with the same value would mean the iterations
        # shared a parameter, which reports SUCCESS and is wrong.
        found = tables(host_root)
        missing = set(INPUTS) - found
        if missing:
            raise SystemExit(
                f"for_each: inputs {sorted(missing)} left no table; disk holds {sorted(found)} — "
                f"the loop ran {len(found)} of {len(INPUTS)} iterations"
            )
        for name in INPUTS:
            ran = read_ran(host_root, name)
            if ran != [name]:
                raise SystemExit(
                    f"for_each: the table for {name!r} holds {ran} — the iteration ran with "
                    f"another input's value, so every iteration saw the same one"
                )
        print(f"   for_each_task: {len(INPUTS)} inputs, {len(INPUTS)} tables, each holding its own value")

        # --- run_job_task ---------------------------------------------------
        child = w.jobs.create(
            name="e2e-comp-child",
            tasks=[jobs.Task(
                task_key="work",
                spark_python_task=jobs.SparkPythonTask(
                    python_file=f"{WORKSPACE_DIR}/task.py", parameters=["child"]
                ),
            )],
        )
        parent = w.jobs.create(
            name="e2e-comp-parent",
            tasks=[jobs.Task(task_key="call", run_job_task=jobs.RunJobTask(job_id=child.job_id))],
        )
        run = w.jobs.run_now(job_id=parent.job_id)
        run_id = int(run.run_id if hasattr(run, "run_id") else run.response.run_id)
        got = wait_run(w, run_id)
        result = str(getattr(got.state.result_state, "value", got.state.result_state))
        if result != "SUCCESS":
            raise SystemExit(f"run_job run ended {result}: {got.state}")

        # The parent writes nothing of its own, so this table exists only if the
        # child's task reached the engine.
        if "child" not in tables(host_root):
            raise SystemExit(
                "run_job_task: the parent reported SUCCESS but the child left no table, "
                f"so nothing ran; disk holds {sorted(tables(host_root))}"
            )
        if read_ran(host_root, "child") != ["child"]:
            raise SystemExit("run_job_task: the child's table holds the wrong value")

        # And the child run is REACHABLE through the SDK, which is what makes a
        # green parent checkable rather than merely green.
        # Read through the SDK's own accessor, which is what settles WHERE the
        # field belongs: `RunTask` has no run_job_output at all — the client
        # looks in jobs/runs/get-output.
        output = w.jobs.get_run_output(run_id=int(run_id))
        child_run_id = getattr(getattr(output, "run_job_output", None), "run_id", None)
        if not child_run_id:
            raise SystemExit(f"run_job_task: no run_job_output.run_id on get_run_output: {output}")
        fetched = w.jobs.get_run(run_id=int(child_run_id))
        if fetched.job_id != child.job_id:
            raise SystemExit(
                f"run_job_output.run_id {child_run_id} resolves to job {fetched.job_id}, "
                f"not the child {child.job_id}"
            )
        print(f"   run_job_task: child ran, run {child_run_id} fetched back through the SDK")

        print(
            "unmodified databricks-sdk drove for_each_task and run_job_task; delta-rs confirmed "
            "each iteration got its own input and that the child job really ran"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
