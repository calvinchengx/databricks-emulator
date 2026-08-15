package server

import (
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestJobsPythonRunsAndMutation(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/jobs/load.py", []byte("print('REACHED')\n"), "NOTEBOOK", "PYTHON")
	var created map[string]any
	if st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "load",
		"tasks": []map[string]any{{
			"task_key": "py",
			"spark_python_task": map[string]any{
				"python_file": "/jobs/load.py",
				"parameters":  []string{"--full"},
			},
		}},
	}, &created); st != 200 {
		t.Fatalf("create %d", st)
	}
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	out := h.waitRun(int64(run["run_id"].(float64)))
	state := out["state"].(map[string]any)
	if state["result_state"] != "SUCCESS" {
		t.Fatalf("run %+v", out)
	}
	if !strings.Contains(str(out["executedBy"]), "not a Databricks cluster") {
		t.Fatalf("executedBy %+v", out)
	}
	if len(h.exec.Calls) == 0 || !strings.Contains(h.exec.Calls[0].Code, "print('REACHED')") {
		t.Fatalf("file never reached the engine: %+v", h.exec.Calls)
	}
	if !strings.Contains(h.exec.Calls[0].Code, "sys.argv") || !strings.Contains(h.exec.Calls[0].Code, "--full") {
		t.Fatalf("argv not delivered: %s", h.exec.Calls[0].Code)
	}
	var logs map[string]any
	h.json("GET", "/api/2.2/jobs/runs/get-output?run_id="+itoa(int64(run["run_id"].(float64))), pat, nil, &logs)
	if logs["logs"] == nil {
		t.Fatalf("output %+v", logs)
	}
}

func TestJobsNotebookBindsNames(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/nb.py", []byte("print(run_date)\n"), "NOTEBOOK", "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.1/jobs/create", pat, map[string]any{
		"name": "nb",
		"tasks": []map[string]any{{
			"task_key": "n",
			"notebook_task": map[string]any{
				"notebook_path":   "/nb.py",
				"base_parameters": map[string]any{"run_date": "2026-08-08"},
			},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.1/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.waitRun(int64(run["run_id"].(float64)))
	if len(h.exec.Calls) == 0 || !strings.Contains(h.exec.Calls[0].Code, "globals()[__k]") {
		t.Fatalf("notebook params: %+v", h.exec.Calls)
	}
}

func TestJobsRefuseUnsupportedAtCreate(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	cases := []map[string]any{
		{"task_key": "j", "spark_jar_task": map[string]any{"main_class_name": "Foo"}},
		{"task_key": "d", "dbt_task": map[string]any{"project_directory": "/"}},
		{"task_key": "p", "pipeline_task": map[string]any{"pipeline_id": "x"}},
		{"task_key": "s", "sql_task": map[string]any{"file": map[string]any{"path": "/q.sql"}}},
		{"task_key": "l", "spark_python_task": map[string]any{"python_file": "/x.py"}, "libraries": []any{map[string]any{"pypi": map[string]any{"package": "x"}}}},
	}
	for _, task := range cases {
		var body map[string]any
		st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{"name": "bad", "tasks": []any{task}}, &body)
		if st != 400 {
			t.Fatalf("task %v → %d %+v", task, st, body)
		}
	}
}

func TestJobsDiamondALLSUCCESSSkips(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/a.py", []byte("a"), "FILE", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/b.py", []byte("b"), "FILE", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/c.py", []byte("c"), "FILE", "PYTHON")
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if strings.Contains(req.Code, "a\n") || strings.HasSuffix(strings.TrimSpace(req.Code), "a") {
			return spark.Result{OK: false, EValue: "upstream failed"}, nil
		}
		return spark.Result{OK: true, Stdout: "ok"}, nil
	}
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "diamond",
		"tasks": []map[string]any{
			{"task_key": "a", "spark_python_task": map[string]any{"python_file": "/a.py"}},
			{"task_key": "b", "depends_on": []map[string]any{{"task_key": "a"}}, "run_if": "ALL_SUCCESS",
				"spark_python_task": map[string]any{"python_file": "/b.py"}},
			{"task_key": "c", "depends_on": []map[string]any{{"task_key": "a"}}, "run_if": "ALL_DONE",
				"spark_python_task": map[string]any{"python_file": "/c.py"}},
		},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	out := h.waitRun(int64(run["run_id"].(float64)))
	states := map[string]string{}
	for _, raw := range out["tasks"].([]any) {
		m := raw.(map[string]any)
		st := m["state"].(map[string]any)
		states[str(m["task_key"])] = str(st["result_state"])
	}
	if states["a"] != "FAILED" || states["b"] != "SKIPPED" || states["c"] != "SUCCESS" {
		t.Fatalf("diamond states %+v", states)
	}
}

func TestJobsNoEngineFails(t *testing.T) {
	h := newHarness(t)
	h.srv.Spark = nil
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/x.py", []byte("print(1)"), "FILE", "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "x", "tasks": []map[string]any{{"task_key": "t", "spark_python_task": map[string]any{"python_file": "/x.py"}}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	out := h.waitRun(int64(run["run_id"].(float64)))
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("no engine %+v", out)
	}
}

func TestJobsCRUDAndCancel(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/x.py", []byte("print(1)"), "FILE", "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "x", "tasks": []map[string]any{{"task_key": "t", "spark_python_task": map[string]any{"python_file": "/x.py"}}},
	}, &created)
	h.json("GET", "/api/2.2/jobs/get?job_id="+itoa(int64(created["job_id"].(float64))), pat, nil, nil)
	h.json("GET", "/api/2.2/jobs/list", pat, nil, nil)
	h.json("POST", "/api/2.2/jobs/reset", pat, map[string]any{
		"job_id": created["job_id"],
		"new_settings": map[string]any{
			"name": "y", "tasks": []map[string]any{{"task_key": "t", "spark_python_task": map[string]any{"python_file": "/x.py"}}},
		},
	}, nil)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.json("POST", "/api/2.2/jobs/runs/cancel", pat, map[string]any{"run_id": run["run_id"]}, nil)
	h.json("GET", "/api/2.2/jobs/runs/list?job_id="+itoa(int64(created["job_id"].(float64))), pat, nil, nil)
	if st := h.json("POST", "/api/2.2/jobs/delete", pat, map[string]any{"job_id": created["job_id"]}, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
}
