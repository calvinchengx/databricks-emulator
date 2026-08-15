#!/usr/bin/env python3
"""Drive unmodified databricks-sdk against the emulator with Sail attached."""

from __future__ import annotations

import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
HOST = "http://127.0.0.1:18449"
AGENT = "http://127.0.0.1:8099"
VAULT_HOST = "127.0.0.1:28444"
ENTRA = "https://127.0.0.1:28443"
TENANT = "6f89cf12-978b-4d23-ac18-9ef0c127cf87"
CLIENT_ID = "00d88624-f0d7-46f6-a641-6232c2608928"
CLIENT_SECRET = "daemon-app-secret"
TLS = ssl._create_unverified_context()
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-engine"]


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


def start_emulator(bin_path: Path, data_dir: Path) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data_dir)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18449"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    env["DATABRICKS_SPARK_CONNECT_GRPC_URL"] = "http://127.0.0.1:50051"
    env["DATABRICKS_AKV_VAULT_HOST"] = VAULT_HOST
    env["DATABRICKS_AKV_TLS_INSECURE"] = "1"
    env["DATABRICKS_OIDC_TLS_INSECURE"] = "1"
    env["DATABRICKS_ENTRA_TOKEN_URL"] = f"{ENTRA}/{TENANT}/oauth2/v2.0/token"
    env["DATABRICKS_ENTRA_CLIENT_ID"] = CLIENT_ID
    env["DATABRICKS_ENTRA_CLIENT_SECRET"] = CLIENT_SECRET
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_http(HOST + "/health")
    return proc


def entra_token(scope: str) -> str:
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": scope,
    }).encode()
    req = urllib.request.Request(
        f"{ENTRA}/{TENANT}/oauth2/v2.0/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, context=TLS) as resp:
        out = json.loads(resp.read())
    tok = out.get("access_token")
    if not tok:
        raise SystemExit(f"entra returned no access_token: {out}")
    return tok


def vault_put(token: str, name: str, value: str) -> None:
    req = urllib.request.Request(
        f"https://{VAULT_HOST}/secrets/{name}?api-version=7.4",
        data=json.dumps({"value": value}).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, context=TLS) as resp:
        if resp.status not in (200, 201):
            raise SystemExit(f"vault put {name}: {resp.status}")


def vault_unauthenticated_is_401() -> None:
    req = urllib.request.Request(f"https://{VAULT_HOST}/secrets/pw?api-version=7.4")
    try:
        urllib.request.urlopen(req, context=TLS)
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            return
        raise SystemExit(f"vault without bearer: {exc.code}")
    raise SystemExit("vault accepted an unauthenticated GET")


def connect_not_501(pat: str, cluster_id: str) -> None:
    """gRPC URL is wired. urllib is this repo's client — not a Connect witness."""
    req = urllib.request.Request(
        HOST + "/spark.connect.SparkConnectService/AnalyzePlan",
        data=b"\x00\x00\x00\x00\x00",
        headers={
            "Authorization": "Bearer " + pat,
            "Content-Type": "application/grpc",
            "x-databricks-cluster-id": cluster_id,
            "TE": "trailers",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            code = resp.status
            ct = resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        code = exc.code
        ct = exc.headers.get("Content-Type", "")
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        # Empty gRPC frame may RST at Sail. Not 501 means the gRPC URL is wired.
        return
    if code == 501:
        raise SystemExit("connect 501 — DATABRICKS_SPARK_CONNECT_GRPC_URL not wired")
    text = raw.decode(errors="replace")
    if "json" in ct.lower() and "DATABRICKS_SPARK_CONNECT" in text:
        raise SystemExit(f"connect hit the HTTP-agent refusal: {text[:200]}")


def stop(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def mcp(pat: str, body: dict, session: str = "") -> tuple[int, dict, str]:
    req = urllib.request.Request(
        HOST + "/api/2.0/mcp/sql",
        data=json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + pat, "Content-Type": "application/json"},
        method="POST",
    )
    if session:
        req.add_header("Mcp-Session-Id", session)
    try:
        with urllib.request.urlopen(req) as resp:
            sid = resp.headers.get("Mcp-Session-Id") or session
            return resp.status, json.loads(resp.read()), sid
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}"), session


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import DatabricksError
    from databricks.sdk.service.compute import ClusterSpec
    from databricks.sdk.service.jobs import SparkPythonTask, Task
    from databricks.sdk.service.workspace import (
        AzureKeyVaultSecretScopeMetadata,
        ImportFormat,
        ScopeBackendType,
    )

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-engine-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    proc = None
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True)
        wait_http(AGENT + "/health")
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        w = WorkspaceClient(host=HOST, token=pat)

        created = w.clusters.create(
            cluster_name="e2e",
            spark_version="emulator-spark",
            node_type_id="emulator.session",
            num_workers=0,
        ).result()
        if created.state is None or created.state.value != "RUNNING":
            raise SystemExit(f"cluster state {created.state}")
        connect_not_501(pat, created.cluster_id)

        w.workspace.upload("/Shared/reached.py", b"print('REACHED')\n", overwrite=True, format=ImportFormat.AUTO)
        job = w.jobs.create(
            name="e2e-py",
            tasks=[Task(task_key="py", spark_python_task=SparkPythonTask(python_file="/Shared/reached.py"))],
        )
        run = w.jobs.run_now_and_wait(job_id=job.job_id)
        if run.state is None or run.state.result_state is None or run.state.result_state.value != "SUCCESS":
            raise SystemExit(f"job run {run.state}")
        logs = w.jobs.get_run_output(run_id=run.run_id)
        text = (logs.logs or "") + (logs.error or "")
        if "REACHED" not in text:
            raise SystemExit(f"engine never printed REACHED: {text!r}")

        w.secrets.create_scope(scope="kv")
        w.secrets.put_secret(scope="kv", key="pw", string_value="s3cret")
        missing = w.jobs.create(
            name="missing-secret",
            tasks=[Task(
                task_key="t",
                spark_python_task=SparkPythonTask(python_file="/Shared/reached.py"),
                new_cluster=ClusterSpec(spark_env_vars={"X": "{{secrets/kv/nope}}"}),
            )],
        )
        try:
            bad = w.jobs.run_now_and_wait(job_id=missing.job_id)
        except DatabricksError as exc:
            if "secret" not in str(exc).lower() and "FAILED" not in str(exc):
                raise SystemExit(f"missing secret: {exc}")
        else:
            if bad.state and bad.state.result_state and bad.state.result_state.value != "FAILED":
                raise SystemExit(f"missing secret succeeded: {bad.state}")

        w.workspace.upload(
            "/Shared/secret.py",
            b"import os\nprint('SECRET=' + os.environ['PW'])\n",
            overwrite=True,
            format=ImportFormat.AUTO,
        )
        injected = w.jobs.create(
            name="inject-secret",
            tasks=[Task(
                task_key="t",
                spark_python_task=SparkPythonTask(python_file="/Shared/secret.py"),
                new_cluster=ClusterSpec(spark_env_vars={"PW": "{{secrets/kv/pw}}"}),
            )],
        )
        inj = w.jobs.run_now_and_wait(job_id=injected.job_id)
        if inj.state is None or inj.state.result_state is None or inj.state.result_state.value != "SUCCESS":
            raise SystemExit(f"inject run {inj.state}")
        inj_logs = w.jobs.get_run_output(run_id=inj.run_id)
        inj_text = (inj_logs.logs or "") + (inj_logs.error or "")
        if "SECRET=s3cret" not in inj_text:
            raise SystemExit(f"engine never printed the resolved secret: {inj_text!r}")

        vault_unauthenticated_is_401()
        vtok = entra_token("https://vault.azure.net/.default")
        vault_put(vtok, "pw", "vault-one")
        w.secrets.create_scope(
            scope="akv",
            scope_backend_type=ScopeBackendType.AZURE_KEYVAULT,
            backend_azure_keyvault=AzureKeyVaultSecretScopeMetadata(
                resource_id="/subscriptions/x/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/emulator",
                dns_name="https://" + VAULT_HOST,
            ),
        )
        try:
            w.secrets.put_secret(scope="akv", key="pw", string_value="nope")
            raise SystemExit("put on AKV scope must be refused")
        except DatabricksError:
            pass
        akv_job = w.jobs.create(
            name="akv-readthrough",
            tasks=[Task(
                task_key="t",
                spark_python_task=SparkPythonTask(python_file="/Shared/secret.py"),
                new_cluster=ClusterSpec(spark_env_vars={"PW": "{{secrets/akv/pw}}"}),
            )],
        )
        first = w.jobs.run_now_and_wait(job_id=akv_job.job_id)
        if first.state is None or first.state.result_state is None or first.state.result_state.value != "SUCCESS":
            raise SystemExit(f"akv first run {first.state}")
        first_logs = (w.jobs.get_run_output(run_id=first.run_id).logs or "")
        if "SECRET=vault-one" not in first_logs:
            raise SystemExit(f"vault value not printed: {first_logs!r}")
        vault_put(vtok, "pw", "vault-two")
        second = w.jobs.run_now_and_wait(job_id=akv_job.job_id)
        if second.state is None or second.state.result_state is None or second.state.result_state.value != "SUCCESS":
            raise SystemExit(f"akv rotate run {second.state}")
        second_logs = (w.jobs.get_run_output(run_id=second.run_id).logs or "")
        if "SECRET=vault-two" not in second_logs:
            raise SystemExit(f"rotate did not read through: {second_logs!r}")

        wh = w.warehouses.create(name="e2e").result()
        if wh.id is None:
            raise SystemExit("warehouse id missing")
        stmt = w.statement_execution.execute_statement(warehouse_id=wh.id, statement="SELECT 1")
        if stmt.status is None or stmt.status.state is None or stmt.status.state.value not in {"SUCCEEDED", "SUCCESS"}:
            raise SystemExit(f"sql statement {stmt}")
        # Official SDK drops unknown fields; dialect lives on the wire.
        req = urllib.request.Request(
            HOST + "/api/2.0/sql/statements/" + stmt.statement_id,
            headers={"Authorization": "Bearer " + pat},
        )
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode()
        if "spark-sql" not in raw:
            raise SystemExit(f"sql dialect missing: {raw}")

        st, init, sid = mcp(pat, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}})
        if st != 200 or not sid:
            raise SystemExit(f"mcp init {st} {init} sid={sid}")
        st, execd, _ = mcp(pat, {
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "execute_sql",
                "arguments": {"query": "SELECT 1"},
                "_meta": {"warehouse_id": wh.id},
            },
        }, sid)
        blob = json.dumps(execd)
        if st != 200 or "SUCCEEDED" not in blob or "spark-sql" not in blob:
            raise SystemExit(f"mcp execute {st} {execd}")

        print("e2e/engine: cluster + REACHED + secret print + AKV rotate + sql + mcp ok")
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False)


if __name__ == "__main__":
    sys.exit(main())
