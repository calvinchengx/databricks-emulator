#!/usr/bin/env python3
"""Sail writes Delta through the warehouse API; delta-rs confirms the log.

The engine that wrote is never the one that confirms. A warehouse COUNT(*)
after INSERT is not a witness. Three-part INSERT INTO cat.sch.tbl is the
same rule: Sail's unity provider resolves the name against UC OSS on the
Compose network; delta-rs reads the files.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
HOST = "http://127.0.0.1:18451"
AGENT = "http://127.0.0.1:18100"
UC = "http://127.0.0.1:18452"
TABLE = "file:///data/delta/e2e/events"
RACE = "file:///data/delta/e2e/race"
COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), "-p", "dbx-e2e-delta"]


def wait_http(url: str, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError, urllib.error.HTTPError) as exc:
            last = exc
            time.sleep(0.2)
    raise SystemExit(f"never healthy at {url}: {last}")


def start_emulator(bin_path: Path, data_dir: Path) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["DATABRICKS_DISABLE_TLS"] = "1"
    env["DATABRICKS_DATA_DIR"] = str(data_dir)
    env["DATABRICKS_ADDR"] = "127.0.0.1:18451"
    env["DATABRICKS_PUBLIC_URL"] = HOST
    env["DATABRICKS_SPARK_CONNECT_URL"] = AGENT
    env["DATABRICKS_UC_URL"] = UC
    proc = subprocess.Popen([str(bin_path)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    wait_http(HOST + "/health")
    return proc


def stop(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def exec_sql(w, warehouse_id: str, statement: str) -> tuple[str | None, str]:
    stmt = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement)
    state = stmt.status.state.value if stmt.status and stmt.status.state else None
    err = ""
    if stmt.status and getattr(stmt.status, "error", None) is not None:
        err = str(stmt.status.error)
    return state, err or str(stmt)


def sql(w, warehouse_id: str, statement: str) -> None:
    state, err = exec_sql(w, warehouse_id, statement)
    if state not in {"SUCCEEDED", "SUCCESS"}:
        raise SystemExit(f"sql {statement!r}: state={state} {err}")


def confirm(host_table: Path, want_rows: list[tuple[int, str]], min_version: int) -> int:
    """Read with delta-rs. Sail must not be imported here."""
    if "pyspark" in sys.modules or "sail" in sys.modules:
        raise SystemExit("confirmer must not import the writer")
    from deltalake import DeltaTable

    log = host_table / "_delta_log"
    if not log.is_dir():
        listing = list(host_table.rglob("*")) if host_table.exists() else []
        raise SystemExit(
            f"Sail reported success but no _delta_log at {host_table}: {listing[:20]}"
        )
    dt = DeltaTable(str(host_table))
    version = dt.version()
    if version < min_version:
        raise SystemExit(f"delta log version {version} < {min_version}")
    # PyArrow is a deltalake dependency. pandas is not; to_pandas() dies in CI.
    table = dt.to_pyarrow_table()
    cols = {c.lower(): c for c in table.column_names}
    if "id" not in cols or "name" not in cols:
        raise SystemExit(f"delta-rs columns {list(table.column_names)} missing id/name")
    got = sorted(
        (int(i), str(n))
        for i, n in zip(table.column(cols["id"]).to_pylist(), table.column(cols["name"]).to_pylist())
    )
    want = sorted(want_rows)
    if got != want:
        raise SystemExit(f"delta-rs rows {got} != {want}")
    return version


def log_stats(host_table: Path) -> tuple[int, int]:
    """Active data-file count from delta-rs, not a directory listing."""
    from deltalake import DeltaTable

    dt = DeltaTable(str(host_table))
    return dt.version(), len(dt.file_uris())


def main() -> int:
    from databricks.sdk import WorkspaceClient

    host_root = Path(tempfile.mkdtemp(prefix="dbx-delta-"))
    host_table = host_root / "e2e" / "events"
    host_table.parent.mkdir(parents=True)
    for p in (host_root, host_table.parent):
        os.chmod(p, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    data_dir = Path(tempfile.mkdtemp(prefix="dbx-delta-emu-"))
    bin_path = data_dir / "databricks-emulator"
    subprocess.check_call(["go", "build", "-o", str(bin_path), "./cmd/databricks-emulator"], cwd=ROOT)
    proc = None
    env = os.environ.copy()
    env["DELTA_DATA"] = str(host_root)
    try:
        subprocess.run(COMPOSE + ["up", "-d", "--wait"], check=True, env=env)
        wait_http(AGENT + "/health")
        wait_http(UC + "/api/2.1/unity-catalog/catalogs", timeout=120)
        proc = start_emulator(bin_path, data_dir)
        pat = (data_dir / "admin.pat").read_text().strip()
        w = WorkspaceClient(host=HOST, token=pat)
        wh = w.warehouses.create(name="e2e-delta").result()
        if wh.id is None:
            raise SystemExit("warehouse id missing")

        sql(w, wh.id, f"CREATE TABLE events (id INT, name STRING) USING delta LOCATION '{TABLE}'")
        sql(w, wh.id, "INSERT INTO events VALUES (1, 'alice'), (2, 'bob')")
        v1 = confirm(host_table, [(1, "alice"), (2, "bob")], min_version=0)

        sql(w, wh.id, "INSERT INTO events VALUES (3, 'carol')")
        v2 = confirm(host_table, [(1, "alice"), (2, "bob"), (3, "carol")], min_version=v1 + 1)
        if v2 <= v1:
            raise SystemExit(f"second insert did not advance the log: {v1} -> {v2}")

        sql(w, wh.id, "DELETE FROM events WHERE id = 2")
        v3 = confirm(host_table, [(1, "alice"), (3, "carol")], min_version=v2 + 1)
        if v3 <= v2:
            raise SystemExit(f"DELETE did not advance the log: {v2} -> {v3}")

        sql(
            w,
            wh.id,
            """
            MERGE INTO events AS t
            USING (SELECT * FROM VALUES (3, 'carol-upd'), (4, 'dave') AS s(id, name)) AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.name = s.name
            WHEN NOT MATCHED THEN INSERT *
            """,
        )
        v4 = confirm(host_table, [(1, "alice"), (3, "carol-upd"), (4, "dave")], min_version=v3 + 1)
        if v4 <= v3:
            raise SystemExit(f"MERGE did not advance the log: {v3} -> {v4}")

        # Standalone UPDATE: Sail must fail loudly or actually write. A
        # SUCCEEDED no-op is the lookalike this slice refuses.
        after_dml = [(1, "alice"), (3, "carol-upd"), (4, "dave")]
        head = v4
        state, err = exec_sql(w, wh.id, "UPDATE events SET name = 'zed' WHERE id = 1")
        if state in {"SUCCEEDED", "SUCCESS"}:
            after_dml = [(1, "zed"), (3, "carol-upd"), (4, "dave")]
            head = confirm(host_table, after_dml, min_version=head + 1)
            dml = f"{v1} then {v2} then DELETE {v3} then MERGE {v4} then UPDATE {head}"
        else:
            confirm(host_table, after_dml, min_version=head)
            if "SUCCEEDED" in err:
                raise SystemExit(f"UPDATE failed but named success: {err}")
            dml = f"{v1} then {v2} then DELETE {v3} then MERGE {v4}; UPDATE refused ({err})"

        # Same files, three-part name. SDK registers the EXTERNAL table in
        # UC OSS; Sail's unity provider resolves e2e.s.events on the Compose
        # network. The LOCATION write above created _delta_log; an empty
        # UC location is not a Delta table yet.
        from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName, DataSourceFormat, TableType

        w.catalogs.create(name="e2e")
        w.schemas.create(name="s", catalog_name="e2e")
        w.tables.create(
            name="events",
            catalog_name="e2e",
            schema_name="s",
            table_type=TableType.EXTERNAL,
            data_source_format=DataSourceFormat.DELTA,
            storage_location=TABLE,
            columns=[ColumnInfo(
                name="id",
                type_name=ColumnTypeName.INT,
                type_text="int",
                type_json='{"name":"id","type":"integer","nullable":true,"metadata":{}}',
                position=0,
                nullable=True,
            ), ColumnInfo(
                name="name",
                type_name=ColumnTypeName.STRING,
                type_text="string",
                type_json='{"name":"name","type":"string","nullable":true,"metadata":{}}',
                position=1,
                nullable=True,
            )],
        )
        got = w.tables.get("e2e.s.events")
        loc = (got.storage_location or "").rstrip("/")
        if loc != TABLE.rstrip("/"):
            raise SystemExit(f"UC storage_location {got.storage_location!r} != {TABLE}")

        sql(w, wh.id, "INSERT INTO e2e.s.events VALUES (5, 'erin')")
        after_rows = after_dml + [(5, "erin")]
        v_uc = confirm(host_table, after_rows, min_version=head + 1)
        if v_uc <= head:
            raise SystemExit(f"three-part INSERT did not advance the log: {head} -> {v_uc}")

        # OPTIMIZE / VACUUM: Sail has no grammar for these. The family's
        # spark-agent routes them through delta-rs (named shim). Address by
        # path: CREATE TABLE … (cols) USING delta LOCATION is not recorded
        # by the agent's name→location regex, and DESCRIBE DETAIL is not in
        # Sail's grammar. ZORDER is refused rather than silently ignored.
        target = f"delta.`{TABLE}`"
        before_v, before_n = log_stats(host_table)
        sql(w, wh.id, f"OPTIMIZE {target}")
        v_opt = confirm(host_table, after_rows, min_version=0)
        after_v, after_n = log_stats(host_table)
        if after_v < before_v:
            raise SystemExit("OPTIMIZE rewound the log")
        if after_v == before_v and after_n >= before_n:
            raise SystemExit(
                f"OPTIMIZE left version {after_v} and {after_n} files (was {before_n})"
            )

        sql(w, wh.id, f"VACUUM {target} RETAIN 0 HOURS")
        confirm(host_table, after_rows, min_version=0)

        state, err = exec_sql(w, wh.id, f"OPTIMIZE {target} ZORDER BY name")
        if state in {"SUCCEEDED", "SUCCESS"}:
            raise SystemExit("ZORDER must be refused by the delta-rs path, not silently compacted")
        if "SUCCEEDED" in err:
            raise SystemExit(f"ZORDER failed but named success: {err}")

        # Two warehouse overwrites, released together. Serialisation is a
        # valid outcome; two writers landing on the same log version is not.
        host_race = host_root / "e2e" / "race"
        host_race.mkdir(parents=True)
        os.chmod(host_race, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        sql(w, wh.id, f"CREATE TABLE race (id INT, name STRING) USING delta LOCATION '{RACE}'")
        sql(w, wh.id, "INSERT INTO race VALUES (0, 'seed')")
        v_seed = confirm(host_race, [(0, "seed")], min_version=0)
        barrier = threading.Barrier(2)
        outcomes: list[tuple[str, str | None, str]] = []

        def overwrite(label: str, values: str) -> None:
            try:
                barrier.wait(timeout=30)
                state, err = exec_sql(w, wh.id, f"INSERT OVERWRITE TABLE race VALUES {values}")
                outcomes.append((label, state, err))
            except Exception as exc:  # noqa: BLE001 — both writers must report
                outcomes.append((label, "EXC", str(exc)))

        threads = [
            threading.Thread(target=overwrite, args=("a", "(1, 'race-a')")),
            threading.Thread(target=overwrite, args=("b", "(2, 'race-b')")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
        ok = [o for o in outcomes if o[1] in {"SUCCEEDED", "SUCCESS"}]
        if not ok:
            raise SystemExit(f"both overwrites failed: {outcomes}")
        from deltalake import DeltaTable

        v_race = DeltaTable(str(host_race)).version()
        if v_race - v_seed != len(ok):
            raise SystemExit(
                f"race log advanced {v_race - v_seed} but successes={len(ok)} outcomes={outcomes}"
            )
        names = {
            str(n) for n in DeltaTable(str(host_race)).to_pyarrow_table().column("name").to_pylist()
        }
        if names == {"race-a"}:
            want_race = [(1, "race-a")]
        elif names == {"race-b"}:
            want_race = [(2, "race-b")]
        else:
            raise SystemExit(f"race rows {names} are not a single overwrite")
        confirm(host_race, want_race, min_version=v_seed + 1)

        print(
            f"e2e/delta: Sail wrote, delta-rs confirmed versions {dml}; "
            f"UC three-part INSERT {v_uc}; OPTIMIZE {before_n}->{after_n} files "
            f"v{before_v}->{v_opt}; VACUUM ok; ZORDER refused ({err}); "
            f"concurrent overwrite {outcomes} log {v_seed}->{v_race}"
        )
        return 0
    finally:
        stop(proc)
        subprocess.run(COMPOSE + ["down", "-v"], check=False, env=env)


if __name__ == "__main__":
    sys.exit(main())
