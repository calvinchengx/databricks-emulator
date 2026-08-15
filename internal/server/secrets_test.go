package server

import (
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestSecretsCRUDGetRefusedAndJobInjection(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{"scope": "kv"}, nil); st != 200 {
		t.Fatalf("scope %d", st)
	}
	if st := h.json("POST", "/api/2.0/secrets/put", pat, map[string]any{"scope": "kv", "key": "pw", "string_value": "s3cret"}, nil); st != 200 {
		t.Fatalf("put %d", st)
	}
	if st := h.json("GET", "/api/2.0/secrets/get?scope=kv&key=pw", pat, nil, nil); st != 400 {
		t.Fatalf("get %d", st)
	}
	var listed map[string]any
	h.json("GET", "/api/2.0/secrets/list?scope=kv", pat, nil, &listed)
	h.json("GET", "/api/2.0/secrets/scopes/list", pat, nil, nil)

	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), "FILE", "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "s",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "/s.py"},
			"new_cluster":       map[string]any{"spark_env_vars": map[string]any{"PW": "{{secrets/kv/pw}}"}},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.waitRun(int64(run["run_id"].(float64)))
	if len(h.exec.Calls) == 0 || h.exec.Calls[0].Env["PW"] != "s3cret" {
		t.Fatalf("secret not injected: %+v", h.exec.Calls)
	}

	h.exec.Calls = nil
	var created2 map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "missing",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "/s.py"},
			"new_cluster":       map[string]any{"spark_env_vars": map[string]any{"X": "{{secrets/kv/nope}}"}},
		}},
	}, &created2)
	var run2 map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created2["job_id"]}, &run2)
	out := h.waitRun(int64(run2["run_id"].(float64)))
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("missing secret %+v", out)
	}

	h.json("POST", "/api/2.0/secrets/delete", pat, map[string]any{"scope": "kv", "key": "pw"}, nil)
	h.json("POST", "/api/2.0/secrets/scopes/delete", pat, map[string]any{"scope": "kv"}, nil)
	if st := h.json("POST", "/api/2.0/secrets/put", pat, map[string]any{"scope": "kv", "key": "pw", "string_value": "x"}, nil); st != 404 {
		t.Fatalf("put after delete %d", st)
	}
}

func TestJobsDBFSPathAndFailedEngine(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.DBFS.Put("/jobs/x.py", []byte("print('dbfs')"))
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if !strings.Contains(req.Code, "print('dbfs')") {
			t.Fatalf("dbfs file not loaded: %s", req.Code)
		}
		return spark.Result{OK: false, EName: "Fail", EValue: "nope"}, nil
	}
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "d",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "dbfs:/jobs/x.py"},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	out := h.waitRun(int64(run["run_id"].(float64)))
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("%+v", out)
	}
}
