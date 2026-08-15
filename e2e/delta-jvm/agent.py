#!/usr/bin/env python3
"""Minimal /statements agent on classic JVM Spark. SPARK_REMOTE is unset."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("dbx-delta-jvm")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, {"state": "idle"})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        n = int(self.headers.get("Content-Length", 0) or 0)
        req = json.loads(self.rfile.read(n) or b"{}")
        if self.path != "/statements":
            self._send(404, {"error": "not found"})
            return
        try:
            df = spark.sql(req.get("code") or "")
            text = ""
            if df.schema.fields:
                text = str(df.collect())
            self._send(200, {"status": "ok", "data": {"text/plain": text}})
        except Exception as exc:  # noqa: BLE001 — surface the engine error
            self._send(200, {"status": "error", "ename": type(exc).__name__, "evalue": str(exc)})

    def _send(self, code: int, body: dict) -> None:
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args) -> None:
        return


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8099), Handler).serve_forever()
