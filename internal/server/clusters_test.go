package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestClustersCreateStartsSailSession(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "python" || !strings.HasPrefix(req.Session, "cluster-") || !strings.Contains(req.Code, "print(1)") {
			t.Fatalf("session ping %+v", req)
		}
		return spark.Result{OK: true, Stdout: "1"}, nil
	}
	var created map[string]any
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{
		"cluster_name": "dev",
	}, &created); st != 200 {
		t.Fatalf("create %d %+v", st, created)
	}
	id := str(created["cluster_id"])
	if id == "" {
		t.Fatalf("id %+v", created)
	}
	if len(h.exec.Calls) == 0 {
		t.Fatal("RUNNING without reaching the engine")
	}
	var got map[string]any
	if st := h.json("GET", "/api/2.0/clusters/get?cluster_id="+id, pat, nil, &got); st != 200 {
		t.Fatalf("get %d", st)
	}
	if got["state"] != "RUNNING" || !strings.Contains(str(got["state_message"]), "not a VM") {
		t.Fatalf("get %+v", got)
	}
	if got["spark_version"] != "emulator-spark" || got["node_type_id"] != "emulator.session" {
		t.Fatalf("echo %+v", got)
	}
	var listed map[string]any
	h.json("GET", "/api/2.0/clusters/list", pat, nil, &listed)
	if len(listed["clusters"].([]any)) != 1 {
		t.Fatalf("list %+v", listed)
	}
	var versions map[string]any
	h.json("GET", "/api/2.0/clusters/spark-versions", pat, nil, &versions)
	if versions["versions"].([]any)[0].(map[string]any)["key"] != "emulator-spark" {
		t.Fatalf("versions %+v", versions)
	}
	var nodes map[string]any
	h.json("GET", "/api/2.0/clusters/list-node-types", pat, nil, &nodes)
	if nodes["node_types"].([]any)[0].(map[string]any)["node_type_id"] != "emulator.session" {
		t.Fatalf("nodes %+v", nodes)
	}
	if st := h.json("POST", "/api/2.0/clusters/delete", pat, map[string]any{"cluster_id": id}, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
}

func TestClustersCreateNoEngineFails(t *testing.T) {
	h := newHarness(t)
	h.srv.Spark = nil
	pat := h.srv.Store.AdminPAT
	var body map[string]any
	if st := h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{
		"cluster_name": "dev",
	}, &body); st != 400 {
		t.Fatalf("no engine %d %+v", st, body)
	}
	if !strings.Contains(str(body["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("message %+v", body)
	}
	var listed map[string]any
	h.json("GET", "/api/2.0/clusters/list", pat, nil, &listed)
	if len(listed["clusters"].([]any)) != 0 {
		t.Fatalf("leaked handle %+v", listed)
	}
}

func TestClustersCreateEngineFailureAndRefuseVM(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{
		"cluster_name": "x", "autoscale": map[string]any{"min_workers": 1, "max_workers": 2},
	}, nil); st != 400 {
		t.Fatalf("autoscale %d", st)
	}
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{
		"cluster_name": "x", "libraries": []any{map[string]any{"pypi": map[string]any{"package": "x"}}},
	}, nil); st != 400 {
		t.Fatalf("libraries %d", st)
	}
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{}, nil); st != 400 {
		t.Fatalf("nameless %d", st)
	}
	if st := h.json("POST", "/api/2.1/clusters/create", pat, `{`, nil); st != 400 {
		t.Fatalf("bad body %d", st)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{}, errors.New("connect refused")
	}
	var dial map[string]any
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{"cluster_name": "x"}, &dial); st != 400 {
		t.Fatalf("dial %d %+v", st, dial)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EValue: "SESSION_FAILED"}, nil
	}
	var fail map[string]any
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{"cluster_name": "x"}, &fail); st != 400 {
		t.Fatalf("fail %d %+v", st, fail)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EName: "BindError"}, nil
	}
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{"cluster_name": "x"}, nil); st != 400 {
		t.Fatalf("ename %d", st)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: false}, nil
	}
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{"cluster_name": "x"}, nil); st != 400 {
		t.Fatalf("empty fail %d", st)
	}
}

func TestClustersStartStopMissingAndBadBodies(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{
		"cluster_name": "dev", "spark_version": "emulator-spark", "node_type_id": "emulator.session",
	}, &created)
	id := str(created["cluster_id"])
	h.srv.Store.Clusters.SetState(id, "TERMINATED", "stopped")
	if st := h.json("POST", "/api/2.0/clusters/start", pat, map[string]any{"cluster_id": id}, nil); st != 200 {
		t.Fatalf("start %d", st)
	}
	var got map[string]any
	h.json("GET", "/api/2.0/clusters/get?cluster_id="+id, pat, nil, &got)
	if got["state"] != "RUNNING" {
		t.Fatalf("restarted %+v", got)
	}

	if st := h.json("GET", "/api/2.0/clusters/get?cluster_id=missing", pat, nil, nil); st != 404 {
		t.Fatalf("get missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/clusters/delete", pat, map[string]any{"cluster_id": "missing"}, nil); st != 404 {
		t.Fatalf("delete missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/clusters/start", pat, map[string]any{"cluster_id": "missing"}, nil); st != 404 {
		t.Fatalf("start missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/clusters/start", pat, `{`, nil); st != 400 {
		t.Fatalf("bad start %d", st)
	}
	if st := h.json("POST", "/api/2.0/clusters/delete", pat, `{`, nil); st != 400 {
		t.Fatalf("bad delete %d", st)
	}

	h.srv.Spark = nil
	if st := h.json("POST", "/api/2.0/clusters/start", pat, map[string]any{"cluster_id": id}, nil); st != 400 {
		t.Fatalf("start no engine %d", st)
	}
}
