package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestSQLWarehouseStatementDialectAndMutation(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{
		"name": "starter", "cluster_size": "2X-Small",
	}, &created); st != 200 {
		t.Fatalf("create %d", st)
	}
	if created["state"] != "RUNNING" || created["cluster_size"] != "2X-Small" {
		t.Fatalf("warehouse %+v", created)
	}
	id := str(created["id"])

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "sql" || req.Code != "SELECT 1" {
			t.Fatalf("engine request %+v", req)
		}
		return spark.Result{OK: true, Stdout: `[{"1":1}]`}, nil
	}
	var execd map[string]any
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "SELECT 1",
	}, &execd); st != 200 {
		t.Fatalf("execute %d %+v", st, execd)
	}
	if execd["dialect"] != "spark-sql" {
		t.Fatalf("dialect %+v", execd)
	}
	if !strings.Contains(str(execd["executedBy"]), "not Photon") {
		t.Fatalf("executedBy %+v", execd)
	}
	status, _ := execd["status"].(map[string]any)
	if status["state"] != "SUCCEEDED" {
		t.Fatalf("status %+v", execd)
	}
	if len(h.exec.Calls) == 0 {
		t.Fatal("SUCCESS without reaching the engine")
	}

	var got map[string]any
	if st := h.json("GET", "/api/2.0/sql/statements/"+str(execd["statement_id"]), pat, nil, &got); st != 200 {
		t.Fatalf("get stmt %d", st)
	}
	if got["dialect"] != "spark-sql" {
		t.Fatalf("get %+v", got)
	}

	var listed map[string]any
	h.json("GET", "/api/2.0/sql/warehouses", pat, nil, &listed)
	h.json("GET", "/api/2.0/sql/warehouses/"+id, pat, nil, nil)
	if st := h.json("POST", "/api/2.0/sql/warehouses/"+id+"/stop", pat, map[string]any{}, nil); st != 200 {
		t.Fatalf("stop %d", st)
	}
	var stopped map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "SELECT 2",
	}, &stopped)
	if stopped["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("stopped still ran %+v", stopped)
	}
	h.json("POST", "/api/2.0/sql/warehouses/"+id+"/start", pat, map[string]any{}, nil)
	if st := h.json("DELETE", "/api/2.0/sql/warehouses/"+id, pat, nil, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
}

func TestSQLStatementNoEngineFails(t *testing.T) {
	h := newHarness(t)
	h.srv.Spark = nil
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "x"}, &created)
	var execd map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": created["id"], "statement": "SELECT 1",
	}, &execd)
	if execd["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("no engine %+v", execd)
	}
	if strings.Contains(str(execd["status"].(map[string]any)["error"]), "SUCCEEDED") {
		t.Fatal("named success")
	}
}

func TestSQLWarehouseMissingAndBadBodies(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{}, nil); st != 400 {
		t.Fatalf("nameless %d", st)
	}
	if st := h.json("GET", "/api/2.0/sql/warehouses/missing", pat, nil, nil); st != 404 {
		t.Fatalf("get missing %d", st)
	}
	if st := h.json("DELETE", "/api/2.0/sql/warehouses/missing", pat, nil, nil); st != 404 {
		t.Fatalf("delete missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{"statement": "SELECT 1"}, nil); st != 400 {
		t.Fatalf("no warehouse %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": "nope", "statement": "SELECT 1",
	}, nil); st != 404 {
		t.Fatalf("unknown warehouse %d", st)
	}
	if st := h.json("GET", "/api/2.0/sql/statements/nope", pat, nil, nil); st != 404 {
		t.Fatalf("unknown stmt %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/warehouses/nope/start", pat, map[string]any{}, nil); st != 404 {
		t.Fatalf("start missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/warehouses/nope/stop", pat, map[string]any{}, nil); st != 404 {
		t.Fatalf("stop missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, `{`, nil); st != 400 {
		t.Fatalf("bad warehouse body %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/statements", pat, `{`, nil); st != 400 {
		t.Fatalf("bad statement body %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": "x", "statement": "",
	}, nil); st != 400 {
		t.Fatalf("empty statement %d", st)
	}
}

func TestSQLWarehouseForwardsDeltaDML(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "dml"}, &created)
	id := str(created["id"])

	statements := []string{
		"DELETE FROM events WHERE id = 2",
		"MERGE INTO events AS t USING updates AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name WHEN NOT MATCHED THEN INSERT *",
		"UPDATE events SET name = 'zed' WHERE id = 1",
		"INSERT OVERWRITE TABLE race VALUES (1, 'race-a')",
	}
	for _, stmt := range statements {
		h.exec.Hook = func(req spark.Request) (spark.Result, error) {
			if req.Kind != "sql" || req.Code != stmt {
				t.Fatalf("engine request %+v want kind=sql code=%q", req, stmt)
			}
			return spark.Result{OK: true, Stdout: "ok"}, nil
		}
		var execd map[string]any
		if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
			"warehouse_id": id, "statement": stmt,
		}, &execd); st != 200 {
			t.Fatalf("%s: %d %+v", stmt, st, execd)
		}
		if execd["status"].(map[string]any)["state"] != "SUCCEEDED" {
			t.Fatalf("forwarded %s: %+v", stmt, execd)
		}
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EValue: "found UPDATE at 0:6 expected something else"}, nil
	}
	var upd map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "UPDATE events SET name = 'zed' WHERE id = 1",
	}, &upd)
	errObj, _ := upd["status"].(map[string]any)["error"].(map[string]any)
	if upd["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("update fail %+v", upd)
	}
	if !strings.Contains(str(errObj["message"]), "UPDATE") {
		t.Fatalf("update error %+v", upd)
	}

	h.srv.Spark = nil
	var missing map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "DELETE FROM events WHERE id = 1",
	}, &missing)
	if missing["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("no engine DELETE %+v", missing)
	}
	errObj, _ = missing["status"].(map[string]any)["error"].(map[string]any)
	if !strings.Contains(str(errObj["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("DELETE must name the missing engine %+v", missing)
	}
}

func TestSQLWarehouseForwardsThreePartNames(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "uc"}, &created)
	id := str(created["id"])

	stmt := "INSERT INTO e2e.s.events VALUES (5, 'erin')"
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "sql" || req.Code != stmt {
			t.Fatalf("three-part rewritten %+v", req)
		}
		return spark.Result{OK: true, Stdout: "ok"}, nil
	}
	var execd map[string]any
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": stmt,
	}, &execd); st != 200 {
		t.Fatalf("execute %d %+v", st, execd)
	}
	if execd["status"].(map[string]any)["state"] != "SUCCEEDED" {
		t.Fatalf("forwarded three-part: %+v", execd)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Code != stmt {
			t.Fatalf("engine request %+v", req)
		}
		return spark.Result{OK: false, EValue: "TABLE_OR_VIEW_NOT_FOUND e2e.s.events"}, nil
	}
	var named map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": stmt,
	}, &named)
	errObj, _ := named["status"].(map[string]any)["error"].(map[string]any)
	if named["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("three-part fail %+v", named)
	}
	if !strings.Contains(str(errObj["message"]), "e2e.s.events") {
		t.Fatalf("three-part error %+v", named)
	}

	h.srv.Spark = nil
	var missing map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": stmt,
	}, &missing)
	if missing["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("no engine three-part %+v", missing)
	}
	errObj, _ = missing["status"].(map[string]any)["error"].(map[string]any)
	if !strings.Contains(str(errObj["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("three-part must name the missing engine %+v", missing)
	}
}

func TestSQLWarehouseForwardsDeltaMaintenance(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "maint"}, &created)
	id := str(created["id"])

	statements := []string{
		"OPTIMIZE events",
		"VACUUM events RETAIN 0 HOURS",
		"OPTIMIZE delta.`file:///data/delta/e2e/events`",
		"VACUUM delta.`file:///data/delta/e2e/events` RETAIN 0 HOURS",
	}
	for _, stmt := range statements {
		h.exec.Hook = func(req spark.Request) (spark.Result, error) {
			if req.Kind != "sql" || req.Code != stmt {
				t.Fatalf("engine request %+v want kind=sql code=%q", req, stmt)
			}
			return spark.Result{OK: true, Stdout: "ok"}, nil
		}
		var execd map[string]any
		if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
			"warehouse_id": id, "statement": stmt,
		}, &execd); st != 200 {
			t.Fatalf("%s: %d %+v", stmt, st, execd)
		}
		if execd["status"].(map[string]any)["state"] != "SUCCEEDED" {
			t.Fatalf("forwarded %s: %+v", stmt, execd)
		}
	}

	zorder := "OPTIMIZE events ZORDER BY name"
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "sql" || req.Code != zorder {
			t.Fatalf("ZORDER rewritten %+v", req)
		}
		return spark.Result{OK: false, EValue: "OPTIMIZE ... ZORDER is not supported by the delta-rs path"}, nil
	}
	var named map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": zorder,
	}, &named)
	errObj, _ := named["status"].(map[string]any)["error"].(map[string]any)
	if named["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("ZORDER fail %+v", named)
	}
	if !strings.Contains(str(errObj["message"]), "ZORDER") {
		t.Fatalf("ZORDER error %+v", named)
	}

	h.srv.Spark = nil
	var missing map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "OPTIMIZE events",
	}, &missing)
	if missing["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("no engine OPTIMIZE %+v", missing)
	}
	errObj, _ = missing["status"].(map[string]any)["error"].(map[string]any)
	if !strings.Contains(str(errObj["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("OPTIMIZE must name the missing engine %+v", missing)
	}
}

func TestSQLStatementEngineFailure(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "x"}, &created)
	id := str(created["id"])

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{}, errors.New("connect refused")
	}
	var dial map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "SELECT 1",
	}, &dial)
	if dial["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("dial %+v", dial)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EValue: "PARSE_SYNTAX_ERROR"}, nil
	}
	var parse map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "SELEC",
	}, &parse)
	errObj, _ := parse["status"].(map[string]any)["error"].(map[string]any)
	if parse["status"].(map[string]any)["state"] != "FAILED" || errObj["message"] != "PARSE_SYNTAX_ERROR" {
		t.Fatalf("parse %+v", parse)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EName: "AnalysisException"}, nil
	}
	var analysis map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": id, "statement": "SELECT * FROM missing",
	}, &analysis)
	errObj, _ = analysis["status"].(map[string]any)["error"].(map[string]any)
	if errObj["message"] != "AnalysisException" {
		t.Fatalf("ename %+v", analysis)
	}
}
