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
		if req.Kind != "sql" || !strings.Contains(req.Code, "SELECT 1") {
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
