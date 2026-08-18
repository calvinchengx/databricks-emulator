package server

import (
	"testing"
)

// A depends_on entry that is not an object is skipped rather than crashing the
// create. The SDK cannot produce this, but the REST surface is open to anything
// that can post JSON, and a panic in the parser would take the process down.
func TestMalformedDependsOnEntryIsIgnored(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/a.py", []byte("a"), "FILE", "PYTHON")

	var created map[string]any
	code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "junk-edge",
		"tasks": []map[string]any{
			{"task_key": "a", "spark_python_task": map[string]any{"python_file": "/a.py"}},
			{"task_key": "b",
				// A string and a number where objects belong, then one real edge.
				"depends_on":        []any{"a", 7, map[string]any{"task_key": "a"}},
				"spark_python_task": map[string]any{"python_file": "/a.py"}},
		},
	}, &created)
	if code != 200 {
		t.Fatalf("create = %d, want the junk entries skipped and the job accepted", code)
	}

	job, ok := h.srv.Store.Jobs.Get(int64(created["job_id"].(float64)))
	if !ok {
		t.Fatal("job not found")
	}
	for _, task := range job.Tasks {
		if task.Key != "b" {
			continue
		}
		// Only the well-formed edge survives. Keeping a zero-valued Dependency
		// for each junk entry would make "b" wait on a task key that is the
		// empty string, which never completes.
		if len(task.DependsOn) != 1 || task.DependsOn[0].Key != "a" {
			t.Fatalf("b depends_on = %+v, want exactly one edge on a", task.DependsOn)
		}
	}
}

// A job with no tasks is refused at create rather than accepted as a run that
// trivially succeeds.
func TestJobWithNoTasksIsRefused(t *testing.T) {
	h := newHarness(t)
	if code := h.json("POST", "/api/2.2/jobs/create", h.srv.Store.AdminPAT,
		map[string]any{"name": "empty", "tasks": []map[string]any{}}, nil); code == 200 {
		t.Fatal("a job with no tasks was accepted")
	}
}

// A dependency cycle terminates with every task SKIPPED instead of spinning.
//
// Nothing rejects a cycle at create, so the runner is what has to notice: once
// no remaining task has its dependencies met, the wave loop cannot make
// progress. Without that branch the loop would never empty `remaining` and the
// run would hang holding the engine. Conditions make this easier to write by
// accident, since an author reasoning about branches is already thinking about
// edges that do not fire.
func TestDependencyCycleSkipsInsteadOfHanging(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/a.py", []byte("a"), "FILE", "PYTHON")

	var created map[string]any
	if code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "cycle",
		"tasks": []map[string]any{
			{"task_key": "a",
				"depends_on":        []map[string]any{{"task_key": "b"}},
				"spark_python_task": map[string]any{"python_file": "/a.py"}},
			{"task_key": "b",
				"depends_on":        []map[string]any{{"task_key": "a"}},
				"spark_python_task": map[string]any{"python_file": "/a.py"}},
		},
	}, &created); code != 200 {
		t.Fatalf("create = %d", code)
	}

	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat,
		map[string]any{"job_id": created["job_id"]}, &run)
	// waitRun fails the test on its own deadline, which is the hang this
	// branch exists to prevent.
	got := h.waitRun(int64(run["run_id"].(float64)))

	for _, raw := range got["tasks"].([]any) {
		m := raw.(map[string]any)
		if st := m["state"].(map[string]any)["result_state"]; st != "SKIPPED" {
			t.Errorf("task %v result_state = %v, want SKIPPED", m["task_key"], st)
		}
	}
}
