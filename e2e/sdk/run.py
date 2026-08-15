#!/usr/bin/env python3
"""Drive unmodified databricks-sdk against a freshly built emulator."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HOST = "http://127.0.0.1:18447"


def wait_health(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url + "/health", timeout=0.5)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    raise SystemExit(f"emulator never became healthy at {url}")


def start(bin_path: Path, data_dir: Path, env: dict[str, str]) -> subprocess.Popen[bytes]:
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_health(HOST)
    return proc


def stop(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


class ForeignIssuer:
    """A local JWKS issuer so the SDK can present a federated JWT.

    This is test setup, not a witness. The witness is WorkspaceClient.me()
    with those tokens. openssl is the signer so e2e/sdk/requirements.txt
    stays the pinned databricks-sdk only.
    """

    def __init__(self, work: Path) -> None:
        self.kid = "e2e-fed"
        self.key = work / "fed.pem"
        subprocess.check_call(["openssl", "genrsa", "-out", str(self.key), "2048"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        mod = subprocess.check_output(["openssl", "rsa", "-in", str(self.key), "-modulus", "-noout"], text=True)
        n = bytes.fromhex(mod.strip().split("=", 1)[1])
        self.jwks = {
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": self.kid,
                "n": b64url(n),
                "e": b64url((65537).to_bytes(3, "big")),
            }]
        }
        body = json.dumps(self.jwks).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path.rstrip("/") != "/jwks.json":
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt: str, *args: object) -> None:
                return

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.issuer = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def mint(self, aud: str, sub: str = "alice", exp_delta: int = 3600) -> str:
        now = int(time.time())
        header = b64url(json.dumps({"alg": "RS256", "typ": "JWT", "kid": self.kid}).encode())
        payload = b64url(json.dumps({
            "iss": self.issuer,
            "aud": aud,
            "sub": sub,
            "preferred_username": sub,
            "iat": now,
            "nbf": now,
            "exp": now + exp_delta,
        }).encode())
        signing = f"{header}.{payload}".encode()
        sig = subprocess.check_output(["openssl", "dgst", "-sha256", "-sign", str(self.key)], input=signing)
        return f"{header}.{payload}.{b64url(sig)}"

    def close(self) -> None:
        self.httpd.shutdown()


def main() -> int:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import DatabricksError
    from databricks.sdk.service.workspace import ImportFormat

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-e2e-"))
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data_dir)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18447"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT, env=env)
    proc = start(bin_path, data_dir, env)
    foreign: ForeignIssuer | None = None
    try:
        pat = (data_dir / "admin.pat").read_text().strip()
        oidc = json.loads((data_dir / "oidc-client.json").read_text())
        w = WorkspaceClient(host=HOST, token=pat)
        me = w.current_user.me()
        if me.user_name != "admin":
            raise SystemExit(f"me.user_name={me.user_name}")

        try:
            WorkspaceClient(host=HOST, token="dev").current_user.me()
        except DatabricksError:
            pass
        else:
            raise SystemExit("token=dev was accepted")

        oauth = WorkspaceClient(
            host=HOST,
            client_id=oidc["client_id"],
            client_secret=oidc["client_secret"],
            auth_type="oauth-m2m",
        )
        oauth_me = oauth.current_user.me()
        if oauth_me.user_name != "admin":
            raise SystemExit(f"oauth me.user_name={oauth_me.user_name}")

        w.workspace.mkdirs("/Shared")
        w.workspace.upload("/Shared/hello.py", b"print(1)\n", overwrite=True, format=ImportFormat.AUTO)
        exported = w.workspace.download("/Shared/hello.py").read()
        if b"print(1)" not in exported:
            raise SystemExit(f"workspace download {exported!r}")

        w.dbfs.put("/tmp/e2e.txt", contents=base64.b64encode(b"dbfs-bytes").decode(), overwrite=True)
        got = w.dbfs.read("/tmp/e2e.txt")
        blob = base64.b64decode(got.data or "")
        if blob != b"dbfs-bytes":
            raise SystemExit(f"dbfs read {got!r}")

        w.secrets.create_scope(scope="kv")
        w.secrets.put_secret(scope="kv", key="pw", string_value="s3cret")
        try:
            w.secrets.get_secret(scope="kv", key="pw")
        except DatabricksError:
            pass
        else:
            raise SystemExit("secrets get was accepted")
        keys = [s.key for s in w.secrets.list_secrets(scope="kv")]
        if "pw" not in keys:
            raise SystemExit(f"secret keys {keys}")

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

        foreign = ForeignIssuer(data_dir)
        good = foreign.mint(HOST)
        try:
            WorkspaceClient(host=HOST, token=good).current_user.me()
        except DatabricksError:
            pass
        else:
            raise SystemExit("unconfigured federated JWT was accepted")

        stop(proc)
        proc = start(bin_path, data_dir, env)
        w2 = WorkspaceClient(host=HOST, token=pat)
        scopes = [s.name for s in w2.secrets.list_scopes()]
        if "kv" not in scopes:
            raise SystemExit(f"scopes after restart {scopes}")
        keys = [s.key for s in w2.secrets.list_secrets(scope="kv")]
        if "pw" not in keys:
            raise SystemExit(f"secrets after restart {keys}")

        stop(proc)
        env["DATABRICKS_OIDC_ISSUERS"] = foreign.issuer
        proc = start(bin_path, data_dir, env)
        fed = WorkspaceClient(host=HOST, token=good).current_user.me()
        if fed.user_name != "alice":
            raise SystemExit(f"federated me.user_name={fed.user_name}")
        for tok, why in (
            (foreign.mint("https://not-databricks"), "wrong aud"),
            (foreign.mint(HOST, exp_delta=-120), "expired"),
            ("aaa.bbb.ccc", "unsigned"),
        ):
            try:
                WorkspaceClient(host=HOST, token=tok).current_user.me()
            except DatabricksError:
                continue
            raise SystemExit(f"federated {why} was accepted")

        print("e2e/sdk: pat + oauth-m2m + federated-jwt + workspace + dbfs + secrets-persist + cluster-refuse ok")
        return 0
    finally:
        stop(proc)
        if foreign is not None:
            foreign.close()


if __name__ == "__main__":
    sys.exit(main())
