package server

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

// A cancelled run must stop dispatching. Before this, cancel only relabelled
// the result: every remaining task still ran against the attached engine, so
// the caller was billed the work they had asked to stop.
func TestCancelStopsDispatchingFurtherTasks(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	for _, name := range []string{"a", "b", "c"} {
		_ = h.srv.Store.Workspace.Put("/"+name+".py", []byte(name), "FILE", "PYTHON")
	}

	var mu sync.Mutex
	var ran []string
	cancelled := make(chan struct{})
	var once sync.Once
	var runID int64

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		// Session is "job-<task key>"; Code carries a preamble and cannot
		// identify the task reliably.
		mu.Lock()
		ran = append(ran, strings.TrimPrefix(req.Session, "job-"))
		mu.Unlock()
		// Cancel while the first wave is in flight, so the second wave is
		// dispatched only if the runner ignores the cancellation.
		once.Do(func() {
			h.json("POST", "/api/2.2/jobs/runs/cancel", pat, map[string]any{"run_id": runID}, nil)
			close(cancelled)
		})
		return spark.Result{OK: true, Stdout: "ok"}, nil
	}

	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "chain",
		"tasks": []map[string]any{
			{"task_key": "a", "spark_python_task": map[string]any{"python_file": "/a.py"}},
			{"task_key": "b", "depends_on": []map[string]any{{"task_key": "a"}},
				"spark_python_task": map[string]any{"python_file": "/b.py"}},
			{"task_key": "c", "depends_on": []map[string]any{{"task_key": "b"}},
				"spark_python_task": map[string]any{"python_file": "/c.py"}},
		},
	}, &created)

	// The hook needs the run id before it fires, so create the run through the
	// store and drive the runner directly with the same job the API built.
	job, ok := h.srv.Store.Jobs.Get(int64(created["job_id"].(float64)))
	if !ok {
		t.Fatal("job not found")
	}
	run := h.srv.Store.Jobs.NewRun(job.ID)
	runID = run.ID
	done := make(chan struct{})
	go func() { h.srv.executeRun(job, run, 0); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeRun did not return after the cancel")
	}
	<-cancelled

	mu.Lock()
	got := append([]string(nil), ran...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("tasks that reached the engine = %v, want only [a] before the cancel stopped dispatch", got)
	}

	final, _ := h.srv.Store.Jobs.GetRun(runID)
	if final.ResultState != "CANCELED" {
		t.Fatalf("ResultState = %q, want CANCELED", final.ResultState)
	}
}

// A run that is never cancelled must still dispatch every wave — the guard
// must not stop ordinary work.
func TestUncancelledRunDispatchesEveryTask(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	for _, name := range []string{"a", "b"} {
		_ = h.srv.Store.Workspace.Put("/"+name+".py", []byte(name), "FILE", "PYTHON")
	}
	var mu sync.Mutex
	var ran int
	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		mu.Lock()
		ran++
		mu.Unlock()
		return spark.Result{OK: true, Stdout: "ok"}, nil
	}

	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "chain2",
		"tasks": []map[string]any{
			{"task_key": "a", "spark_python_task": map[string]any{"python_file": "/a.py"}},
			{"task_key": "b", "depends_on": []map[string]any{{"task_key": "a"}},
				"spark_python_task": map[string]any{"python_file": "/b.py"}},
		},
	}, &created)

	var started map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &started)
	out := h.waitRun(int64(started["run_id"].(float64)))

	mu.Lock()
	got := ran
	mu.Unlock()
	if got != 2 {
		t.Fatalf("tasks dispatched = %d, want 2", got)
	}
	state, _ := out["state"].(map[string]any)
	if str(state["result_state"]) != "SUCCESS" {
		t.Fatalf("result_state = %v, want SUCCESS", state["result_state"])
	}
}
