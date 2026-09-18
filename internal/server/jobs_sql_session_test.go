package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

// A sql_task writes into the warehouse catalog, because that is where it runs.
//
// On Databricks a sql_task runs ON a SQL warehouse -- it is why the kind
// carries no cluster environment -- so a table it creates is one a warehouse
// query can read afterwards. Under "job-"+task key it went to a catalog
// private to that task and the agent dropped it at the end of the run: the
// table existed only for the duration of the job, visible to nobody, not even
// the next task of the same job.
//
// The engine here is the per-session catalog fake from sql_session_test.go,
// which is the agent's behaviour since fabric-emulator v0.33.0.
func TestJobSQLTaskWritesIntoTheWarehouseCatalog(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/make.sql", []byte("CREATE TABLE from_job (id INT)"), "FILE", "SQL")

	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{
		"name": "starter", "cluster_size": "2X-Small",
	}, &created); st != 200 {
		t.Fatalf("create warehouse %d", st)
	}

	engine := &catalogPerSession{}
	var mu sync.Mutex
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		return engine.run(req)
	}

	var job map[string]any
	if st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "sql task",
		"tasks": []map[string]any{{
			"task_key": "make",
			"sql_task": map[string]any{"file": map[string]any{"path": "/make.sql"}},
		}},
	}, &job); st != 200 {
		t.Fatalf("create job %d %+v", st, job)
	}
	var run map[string]any
	if st := h.json("POST", "/api/2.2/jobs/run-now", pat,
		map[string]any{"job_id": job["job_id"]}, &run); st != 200 {
		t.Fatalf("run-now %d %+v", st, run)
	}
	got := h.waitRun(int64(run["run_id"].(float64)))
	if state := str(got["state"].(map[string]any)["result_state"]); state != "SUCCESS" {
		t.Fatalf("job run %s: %+v", state, got)
	}

	// The witness: a warehouse statement, a different surface entirely, using
	// the table the job made.
	var insert map[string]any
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": str(created["id"]), "statement": "INSERT INTO from_job VALUES (1)",
	}, &insert); st != 200 {
		t.Fatalf("insert %d", st)
	}
	if state := statementState(insert); state != "SUCCEEDED" {
		t.Fatalf("a warehouse query could not see the table a sql_task created: %s %+v",
			state, insert["status"])
	}

	mu.Lock()
	defer mu.Unlock()
	if len(engine.tables) != 1 {
		t.Fatalf("job and warehouse opened %d catalogs, want 1: %v", len(engine.tables), engine.tables)
	}
	for _, call := range h.exec.Calls {
		if call.Session != spark.WarehouseSession {
			t.Fatalf("a SQL statement ran in session %q, want %q", call.Session, spark.WarehouseSession)
		}
	}
}

// Python tasks keep a session of their own. Sharing the warehouse session
// would put two tasks' globals in one namespace, which is a different defect
// from the one above -- the REPL boundary is real on Databricks, the catalog
// boundary is not.
func TestPythonTaskKeepsItsOwnSession(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/one.py", []byte("print(1)"), "FILE", "PYTHON")

	var mu sync.Mutex
	var sessions []string
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		mu.Lock()
		sessions = append(sessions, req.Session)
		mu.Unlock()
		return spark.Result{OK: true, Stdout: "1"}, nil
	}

	var job map[string]any
	if st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "python task",
		"tasks": []map[string]any{{
			"task_key":          "py",
			"spark_python_task": map[string]any{"python_file": "/one.py"},
		}},
	}, &job); st != 200 {
		t.Fatalf("create job %d", st)
	}
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": job["job_id"]}, &run)
	got := h.waitRun(int64(run["run_id"].(float64)))
	if state := str(got["state"].(map[string]any)["result_state"]); state != "SUCCESS" {
		t.Fatalf("job run %s", state)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sessions) != 1 || sessions[0] != "job-py" {
		t.Fatalf("python task sessions = %v, want [job-py]", sessions)
	}
}

// A task whose file is not in the workspace fails naming the path, before any
// engine call. The same arm of runTask that chooses the session.
func TestTaskWithMissingFileFailsWithoutReachingTheEngine(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		t.Fatalf("a task with no code reached the engine: %+v", req)
		return spark.Result{}, nil
	}
	var job map[string]any
	if st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "missing",
		"tasks": []map[string]any{{
			"task_key": "gone",
			"sql_task": map[string]any{"file": map[string]any{"path": "/nowhere.sql"}},
		}},
	}, &job); st != 200 {
		t.Fatalf("create job %d %+v", st, job)
	}
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": job["job_id"]}, &run)
	got := h.waitRun(int64(run["run_id"].(float64)))
	tasks := got["tasks"].([]any)
	task := tasks[0].(map[string]any)
	if state := str(task["state"].(map[string]any)["result_state"]); state != "FAILED" {
		t.Fatalf("result_state %s, want FAILED: %+v", state, task)
	}
	if len(h.exec.Calls) != 0 {
		t.Fatalf("engine calls: %d", len(h.exec.Calls))
	}
}

// An engine failure with no message still has to say something: the exception
// NAME is the fallback, because "FAILED" with an empty reason is unreadable.
func TestTaskFailureFallsBackToTheExceptionName(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/boom.py", []byte("raise SystemExit(1)"), "FILE", "PYTHON")
	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{EName: "SystemExit"}, nil // no EValue
	}
	var job map[string]any
	if st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "boom",
		"tasks": []map[string]any{{
			"task_key":          "py",
			"spark_python_task": map[string]any{"python_file": "/boom.py"},
		}},
	}, &job); st != 200 {
		t.Fatalf("create job %d", st)
	}
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": job["job_id"]}, &run)
	runID := int64(run["run_id"].(float64))
	got := h.waitRun(runID)
	task := got["tasks"].([]any)[0].(map[string]any)
	state := task["state"].(map[string]any)
	if str(state["result_state"]) != "FAILED" {
		t.Fatalf("result_state %+v", state)
	}
	// get-output reads the RUN, which is where the SDK looks for stderr.
	var output map[string]any
	if st := h.json("GET", fmt.Sprintf("/api/2.2/jobs/runs/get-output?run_id=%d", runID), pat, nil, &output); st != 200 {
		t.Fatalf("get-output %d", st)
	}
	if str(output["error"]) != "SystemExit" {
		t.Fatalf("error %q, want the exception name", str(output["error"]))
	}
}
