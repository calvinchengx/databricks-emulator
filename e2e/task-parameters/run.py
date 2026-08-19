#!/usr/bin/env python3
"""Two parameterised `spark_python_task`s in ONE wave must each see their own
parameters, and their own environment.

THE SHAPE `e2e/condition-task` COULD NOT USE. That suite gives each arm its own
file and hard-codes the destination, and says why in its docstring: parameters
did not survive a concurrent wave, so building it the obvious way produced two
tasks that both reported SUCCESS while writing the same table. This suite is
that obvious way, run on purpose. ONE file, uploaded once, dispatched twice,
told apart by nothing but `parameters` and `spark_env_vars`.

THE DEFECT IT PINS. `pythonPreamble` (`internal/server/jobs.go`) delivers a
task's parameters as `sys.argv = [...]` and its environment as
`os.environ.update({...})`, prepended to the code sent to the statement agent.
`sys` and `os` are ONE OBJECT EACH per interpreter. The agent gives every Livy
session its own globals, so user variables are isolated -- module state is not.
`runJob` dispatches a wave of independent tasks as concurrent goroutines, so
the second assignment wins for BOTH tasks, and both still report SUCCESS
because nothing failed. The wrong parameters are simply processed. Reproduced
directly against emulator-spark-agent:4.2.0:

    session A: import sys; sys.argv=['a','AAA']; _marker='from-A'
    session B: import sys; print(sys.argv[-1]); print(globals().get('_marker'))
    -> argv: AAA    (leaked)
    -> marker: None (user globals correctly isolated)

`spark_env_vars` is resolved through `{{secrets/...}}` before it is baked in
(`runTask` -> `resolveSecrets`), so the environment half of this leak is one
task reading another task's RESOLVED SECRET, not merely a confused parameter.
That is why this suite witnesses both channels rather than parameters alone.

EXPECTED RED UNTIL THE AGENT DIGEST BUMPS, and measured both ways rather than
assumed. Against the pinned 4.2.0:

    both tasks observed param='param-beta' ...
    argv per task: ['[..., "param-beta"]', '[..., "param-beta"]']

and against a spark-agent built from the fix (see
docker-compose.local-agent.yml for how to reproduce that):

    two parameterised spark_python_tasks ran in one wave; delta-rs confirmed
    each observed its own parameters and its own environment

The fix is agent-side and lives in fabric-emulator
(`python/spark_agent/task_scope.py`); the digest pinned in this directory's
compose file is still the unfixed 4.2.0, so CI does NOT run this suite yet --
wiring a known-red job onto main teaches people to ignore it. Whoever bumps
`SPARK_CLIENT_DIGEST` adds the CI job and turns this green, and at that moment
`e2e/condition-task/run.py`'s argv paragraph becomes history and needs
rewording.

WHY A GO TEST CANNOT REPLACE THIS. `jobs_condition_e2e_test.go` asserts against
`h.exec.Hook`, a fake engine that never executes Python. The defect is in what
the real interpreter does with two statements at once, so a Go witness for it
would be the emulator agreeing with itself.

THE INTERLEAVING IS AN INPUT, NOT A SAMPLE. The task file blocks on a
filesystem barrier until BOTH tasks have run their preamble, then reads. Two
statements that merely start together need not overlap at the moment that
matters, and a leak sampled for is a leak that passes most of the time.
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
PORT = 18460
HOST = f"http://127.0.0.1:{PORT}"
AGENT = "http://127.0.0.1:18107"
TASK_PATH = "/Workspace/params/observe.py"
# SPARK_AGENT_LOCAL_IMAGE swaps the pinned agent for one you built, which is the
# only way to see this suite green before the digest moves. See
# docker-compose.local-agent.yml for why that is a second file and not a second
# variable in the first one.
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml")]
if os.environ.get("SPARK_AGENT_LOCAL_IMAGE"):
    COMPOSE += ["-f", str(HERE / "docker-compose.local-agent.yml")]
COMPOSE += ["-p", "dbx-e2e-params"]

# task_key -> (parameter, environment value). Both are per-task and neither is
# derivable from the other, so a leak in one channel cannot be masked by the
# other happening to be right.
TASKS = {
    "alpha": ("param-alpha", "env-alpha"),
    "beta": ("param-beta", "env-beta"),
}

# ONE file for both tasks. Everything that distinguishes them arrives through
# the two channels under test.
TASK_SOURCE = '''\
import json
import os
import sys
import time
import uuid
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

READY = Path("/data/params/ready")
OBSERVED = Path("/data/params/observed")
READY.mkdir(parents=True, exist_ok=True)
OBSERVED.mkdir(parents=True, exist_ok=True)

# A per-EXECUTION identity taken from neither argv nor the environment -- the
# two channels under test. Deriving it from either would collapse the barrier
# below to a single entry in exactly the case that leaks, turning the
# interesting failure into an uninformative timeout.
token = uuid.uuid4().hex
(READY / token).write_text("1")

# Both tasks must have run the preamble the emulator prepends -- its
# `sys.argv = ...` and `os.environ.update(...)` -- before EITHER reads. Two
# statements dispatched together need not still overlap by the time they read,
# and against a leaky agent that difference is the whole result.
deadline = time.time() + 120
while time.time() < deadline:
    if len(list(READY.iterdir())) >= 2:
        break
    time.sleep(0.1)
else:
    raise RuntimeError("the other task never arrived; this run witnessed no concurrent wave")

seen = {
    "param": sys.argv[1],
    "env": os.environ.get("TASK_PARAM", "<unset>"),
    "argv": json.dumps(sys.argv),
}
print("observed:", seen)

# delta-rs, so the evidence is a table on disk rather than the run record the
# emulator wrote about itself. Keyed by `token`: two tasks that observed the
# SAME value still write two tables, so a collision shows up as a repeated
# value and never as a lost write.
write_deltalake(str(OBSERVED / token), pa.table({k: [v] for k, v in seen.items()}))
'''


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


def upload_task_file(token: str) -> None:
    body = json.dumps(
        {
            "path": TASK_PATH,
            "format": "RAW",
            "overwrite": True,
            "content": base64.b64encode(TASK_SOURCE.encode()).decode(),
        }
    ).encode()
    req = urllib.request.Request(
        HOST + "/api/2.0/workspace/import",
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req).read()


def build_tasks(jobs, ClusterSpec):
    """Two tasks, NO dependency between them, so `runJob` puts both in one wave.

    The parameters go through the SDK's own `SparkPythonTask`, and the
    environment through `ClusterSpec.spark_env_vars`, which is the field
    `resolveSecrets` reads -- so this drives the same path a secret would.
    """
    return [
        jobs.Task(
            task_key=key,
            spark_python_task=jobs.SparkPythonTask(python_file=TASK_PATH, parameters=[param]),
            new_cluster=ClusterSpec(spark_env_vars={"TASK_PARAM": env_value}),
        )
        for key, (param, env_value) in TASKS.items()
    ]


def wait_run(w, run_id: int, timeout: float = 300.0):
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


def read_observations(host_root: Path) -> list[dict[str, str]]:
    """delta-rs reads what each task actually saw."""
    from deltalake import DeltaTable

    observed = host_root / "observed"
    if not observed.exists():
        raise SystemExit(f"no task wrote an observation; volume holds {list(host_root.iterdir())}")
    out = []
    for table_dir in sorted(observed.iterdir()):
        row = DeltaTable(str(table_dir)).to_pyarrow_table().to_pylist()
        if len(row) != 1:
            raise SystemExit(f"{table_dir.name}: expected one observation, got {row}")
        out.append({k: str(v) for k, v in row[0].items()})
    return out


def confirm(observations: list[dict[str, str]]) -> None:
    """Each task saw its OWN parameter and its OWN environment.

    Asserted as sets over both channels: the defect's signature is two tasks
    reporting SUCCESS while holding one task's values, so "the right number of
    tasks ran" and "each ran with the right inputs" are different questions and
    both are asked here.
    """
    if len(observations) != len(TASKS):
        raise SystemExit(
            f"expected {len(TASKS)} task observations, got {len(observations)}: {observations}"
        )

    for channel, expected in (
        ("param", {p for p, _ in TASKS.values()}),
        ("env", {e for _, e in TASKS.values()}),
    ):
        got = sorted(o[channel] for o in observations)
        if len(set(got)) == 1 and len(got) > 1:
            raise SystemExit(
                f"both tasks observed {channel}={got[0]!r}. The wave's second assignment won for "
                f"both: `{'sys.argv' if channel == 'param' else 'os.environ'}` is one object per "
                f"interpreter and the agent isolates user globals but not module state. "
                f"Full argv per task: {[o['argv'] for o in observations]}"
            )
        if sorted(expected) != got:
            raise SystemExit(f"{channel}: expected {sorted(expected)}, tasks observed {got}")


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    from databricks.sdk.service.compute import ClusterSpec

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-params-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(
        ["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT
    )
    host_root = Path(tempfile.mkdtemp(prefix="dbx-params-tables-"))
    os.chmod(host_root, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    env = os.environ.copy()
    env["PARAMS_DATA"] = str(host_root)

    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        upload_task_file(pat)

        w = WorkspaceClient(host=HOST, token=pat)
        created = w.jobs.create(name="e2e-task-parameters", tasks=build_tasks(jobs, ClusterSpec))
        run = w.jobs.run_now(job_id=created.job_id)
        run_id = run.run_id if hasattr(run, "run_id") else run.response.run_id
        got = wait_run(w, int(run_id))

        result = str(getattr(got.state.result_state, "value", got.state.result_state))
        if result != "SUCCESS":
            raise SystemExit(f"run ended {result}: {got.state}")

        confirm(read_observations(host_root))
        print(
            "two parameterised spark_python_tasks ran in one wave; delta-rs confirmed each "
            "observed its own parameters and its own environment"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
