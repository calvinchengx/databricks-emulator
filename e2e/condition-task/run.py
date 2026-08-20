#!/usr/bin/env python3
"""Drive if/else `condition_task` with the unmodified Databricks SDK, and let the
filesystem say which branch ran.

The Go witnesses for this row assert what the fake Spark SAW, which is a real
property but an internal one: our client, our engine stub, our assertions. This
suite replaces every one of those with something outside the emulator. The
unmodified `databricks-sdk` builds the job (`jobs.ConditionTask`,
`jobs.TaskDependency(outcome=...)`), and **delta-rs** reads the result, so the
evidence that the right arm ran is a Delta table on disk rather than the run
record the emulator wrote about itself.

THE PAIR IS THE POINT. Databricks documents two operator families that disagree
on the same operands:

    12.0 EQUAL_TO 12                 -> false   (compared as STRINGS)
    12.0 GREATER_THAN_OR_EQUAL 12    -> true    (compared as NUMBERS)

Either check alone is weak: an emulator that compared everything numerically
would still pass the `>=` case, and one that compared everything as strings
would still pass the `==` case. Run together on identical operands, only the
documented split satisfies both, so a single-family implementation cannot pass
this suite no matter which family it picked.

ABSENCE IS ASSERTED, NOT ASSUMED. Each job writes from BOTH arms, to two
different tables. A branch that should not have run leaves no directory, and the
suite fails if one appears -- otherwise "the right table exists" would also be
true of an emulator that ran both arms.

EACH ARM IS ITS OWN FILE, and deliberately not one file taking a parameter. The
parameter route did not survive this test when it was written: `pythonPreamble`
passes parameters by assigning `sys.argv` on the agent, and `sys` is one module
object per interpreter, so two tasks dispatched in the same wave overwrote each
other's argv and both read the winner's. Building this suite the obvious way is
what surfaced that -- both arms reported SUCCESS while writing the same table.
The agent scopes argv per session now (fabric-emulator
`python/spark_agent/task_scope.py`, witnessed by `e2e/task-parameters`), so the
parameter route would work today. The destination stays hard-coded anyway: it
keeps this suite a test of `condition_task` rather than a hostage to parameter
passing, and leaves the "both arms ran" case detectable instead of collapsing it
into one table.
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
PORT = 18459
HOST = f"http://127.0.0.1:{PORT}"
AGENT = "http://127.0.0.1:18106"
WORKSPACE_DIR = "/Workspace/cond"

# The destination is baked in, for the argv reason in the module docstring.
BRANCH_SOURCE = """import pyarrow as pa
from deltalake import write_deltalake

write_deltalake("/data/cond/{name}", pa.table({{"branch": ["{name}"]}}))
print("branch ran:", "{name}")
"""
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-cond"]

# op, left, right, the arm Databricks' documented semantics select, and why.
CASES = [
    ("EQUAL_TO", "12.0", "12", "false", "EQUAL_TO compares as strings, so 12.0 != 12"),
    ("GREATER_THAN_OR_EQUAL", "12.0", "12", "true", "ordering ops compare as numbers, so 12.0 >= 12"),
]


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


def upload_branch_file(token: str, name: str) -> str:
    """Put one arm's code in the workspace, where a spark_python_task reads it."""
    path = f"{WORKSPACE_DIR}/{name}.py"
    body = json.dumps(
        {
            "path": path,
            "format": "RAW",
            "overwrite": True,
            "content": base64.b64encode(BRANCH_SOURCE.format(name=name).encode()).decode(),
        }
    ).encode()
    req = urllib.request.Request(
        HOST + "/api/2.0/workspace/import",
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req).read()
    return path


def build_job(jobs, name: str, op: str, left: str, right: str):
    """A gate plus both arms. Both write, so a wrongly-taken branch is visible."""
    return [
        jobs.Task(
            task_key="gate",
            # Resolved through the SDK's own enum, so an op it does not define is
            # a failure here rather than a string this emulator might accept.
            condition_task=jobs.ConditionTask(
                op=getattr(jobs.ConditionTaskOp, op), left=left, right=right
            ),
        ),
        jobs.Task(
            task_key="on_true",
            depends_on=[jobs.TaskDependency(task_key="gate", outcome="true")],
            spark_python_task=jobs.SparkPythonTask(python_file=f"{WORKSPACE_DIR}/{name}_true.py"),
        ),
        jobs.Task(
            task_key="on_false",
            depends_on=[jobs.TaskDependency(task_key="gate", outcome="false")],
            spark_python_task=jobs.SparkPythonTask(python_file=f"{WORKSPACE_DIR}/{name}_false.py"),
        ),
    ]


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


def confirm_branch(root: Path, taken: str, not_taken: str) -> None:
    """delta-rs reads the arm that ran; the other must have left nothing behind."""
    from deltalake import DeltaTable

    ran, skipped = root / taken, root / not_taken
    # Two different defects put a table where none belongs, and they are told
    # apart by whether the EXPECTED one is also there. Reporting "both arms ran"
    # for either would name a cause this evidence cannot establish.
    if skipped.exists() and ran.exists():
        raise SystemExit(
            f"both {taken} and {not_taken} exist: `depends_on.outcome` gated nothing "
            f"and each arm ran regardless of the condition"
        )
    if skipped.exists():
        raise SystemExit(
            f"only {not_taken} exists: gating works, but the condition came out the "
            f"opposite way, so the operator's comparison semantics are wrong"
        )
    if not (ran / "_delta_log").is_dir():
        listing = sorted(p.name for p in root.iterdir()) if root.exists() else []
        raise SystemExit(f"the chosen branch {taken} wrote no Delta table; volume holds {listing}")
    table = DeltaTable(str(ran)).to_pyarrow_table()
    got = [str(v) for v in table.column("branch").to_pylist()]
    if got != [taken]:
        raise SystemExit(f"{taken}: delta-rs read {got}, so the wrong arm wrote this table")


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-cond-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(
        ["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT
    )
    host_root = Path(tempfile.mkdtemp(prefix="dbx-cond-tables-"))
    os.chmod(host_root, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    env = os.environ.copy()
    env["COND_DATA"] = str(host_root)

    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        for op, *_ in CASES:
            for arm in ("true", "false"):
                upload_branch_file(pat, f"{op.lower()}_{arm}")

        w = WorkspaceClient(host=HOST, token=pat)

        for op, left, right, expect, why in CASES:
            name = op.lower()
            created = w.jobs.create(name=f"e2e-cond-{name}", tasks=build_job(jobs, name, op, left, right))
            run = w.jobs.run_now(job_id=created.job_id)
            run_id = run.run_id if hasattr(run, "run_id") else run.response.run_id
            got = wait_run(w, int(run_id))
            result = str(getattr(got.state.result_state, "value", got.state.result_state))
            if result != "SUCCESS":
                raise SystemExit(f"{op}: run ended {result}: {got.state}")

            taken = f"{name}_{expect}"
            not_taken = f"{name}_{'false' if expect == 'true' else 'true'}"
            confirm_branch(host_root, taken, not_taken)
            print(f"   {op} {left} {right} -> {expect}: {why}")

        # Said explicitly, because the two cases only mean something together.
        print(
            "unmodified databricks-sdk drove condition_task; delta-rs confirmed the chosen arm "
            "in both operator families on identical operands"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
