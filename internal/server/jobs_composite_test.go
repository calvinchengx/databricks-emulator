package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

// run_job_task and for_each_task: the two Jobs 2.2 task types a real data
// product orchestrates with. Both were refused by name until now, so these
// tests are the first evidence either does anything.

func createJob(t *testing.T, h *harness, name string, tasks []map[string]any) int64 {
	t.Helper()
	var created map[string]any
	if st := h.json("POST", "/api/2.2/jobs/create", h.srv.Store.AdminPAT, map[string]any{
		"name": name, "tasks": tasks,
	}, &created); st != 200 {
		t.Fatalf("create %s: %d", name, st)
	}
	return int64(created["job_id"].(float64))
}

func runToCompletion(t *testing.T, h *harness, jobID int64) map[string]any {
	t.Helper()
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", h.srv.Store.AdminPAT, map[string]any{"job_id": jobID}, &run)
	return h.waitRun(int64(run["run_id"].(float64)))
}

func TestRunJobTaskRunsTheChildAndReportsItsOutcome(t *testing.T) {
	h := newHarness(t)
	_ = h.srv.Store.Workspace.Put("/jobs/child.py", []byte("print('CHILD RAN')\n"), "NOTEBOOK", "PYTHON")

	child := createJob(t, h, "child", []map[string]any{{
		"task_key":          "work",
		"spark_python_task": map[string]any{"python_file": "/jobs/child.py"},
	}})
	parent := createJob(t, h, "parent", []map[string]any{{
		"task_key":     "call_child",
		"run_job_task": map[string]any{"job_id": child},
	}})

	out := runToCompletion(t, h, parent)
	if out["state"].(map[string]any)["result_state"] != "SUCCESS" {
		t.Fatalf("parent run %+v", out)
	}
	// The child's code actually reached the engine — a parent that reported
	// SUCCESS without running anything is the failure this test exists for.
	var ran bool
	for _, c := range h.exec.Calls {
		if strings.Contains(c.Code, "CHILD RAN") {
			ran = true
		}
	}
	if !ran {
		t.Fatalf("the child job's code never reached the engine: %+v", h.exec.Calls)
	}
	// And the child run is REACHABLE: without its id a caller has a green
	// parent and no way to inspect what ran.
	// Read where the SDK reads it: jobs/runs/get-output, not the task in get.
	var output map[string]any
	h.json("GET", fmt.Sprintf("/api/2.2/jobs/runs/get-output?run_id=%v", out["run_id"]),
		h.srv.Store.AdminPAT, nil, &output)
	rj, ok := output["run_job_output"].(map[string]any)
	if !ok || int64(rj["run_id"].(float64)) == 0 {
		t.Fatalf("no run_job_output.run_id on runs/get-output: %+v", output)
	}
	if _, found := h.srv.Store.Jobs.GetRun(int64(rj["run_id"].(float64))); !found {
		t.Fatalf("run_job_output.run_id %v names no run", rj["run_id"])
	}
}

func TestRunJobTaskFailsWhenTheChildFails(t *testing.T) {
	h := newHarness(t)
	h.exec.Hook = func(spark.Request) (spark.Result, error) { return spark.Result{}, fmt.Errorf("boom") }
	_ = h.srv.Store.Workspace.Put("/jobs/child.py", []byte("print('x')\n"), "NOTEBOOK", "PYTHON")
	child := createJob(t, h, "child", []map[string]any{{
		"task_key":          "work",
		"spark_python_task": map[string]any{"python_file": "/jobs/child.py"},
	}})
	parent := createJob(t, h, "parent", []map[string]any{{
		"task_key":     "call_child",
		"run_job_task": map[string]any{"job_id": child},
	}})
	out := runToCompletion(t, h, parent)
	// THE NEGATIVE HALF. A parent that reports SUCCESS over a failed child is
	// worse than one that cannot run children at all.
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("parent reported %+v for a failed child", out["state"])
	}
}

func TestRunJobTaskRefusesAJobThatRunsItself(t *testing.T) {
	h := newHarness(t)
	// A job whose only task runs the job itself. Without the depth bound this
	// spawns runs until the process dies, and the emulator looks hung rather
	// than wrong.
	self := createJob(t, h, "self", []map[string]any{{
		"task_key":     "loop",
		"run_job_task": map[string]any{"job_id": 999999},
	}})
	// Point it at itself now that the id exists.
	job, _ := h.srv.Store.Jobs.Get(self)
	job.Tasks[0].RunJob.JobID = self

	out := runToCompletion(t, h, self)
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("a self-running job did not fail: %+v", out)
	}
	if !strings.Contains(str(out["state"].(map[string]any)["state_message"])+str(out["tasks"]), "nested deeper") {
		t.Fatalf("failed without naming the nesting bound: %+v", out)
	}
}

func TestRunJobTaskFailsOnAnUnknownJob(t *testing.T) {
	h := newHarness(t)
	parent := createJob(t, h, "parent", []map[string]any{{
		"task_key":     "call_child",
		"run_job_task": map[string]any{"job_id": 4242},
	}})
	out := runToCompletion(t, h, parent)
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("an unknown job_id did not fail: %+v", out)
	}
}

func TestForEachTaskRunsOncePerInputWithItsOwnValue(t *testing.T) {
	h := newHarness(t)
	_ = h.srv.Store.Workspace.Put("/jobs/each.py", []byte("print('EACH')\n"), "NOTEBOOK", "PYTHON")
	job := createJob(t, h, "loop", []map[string]any{{
		"task_key": "fan",
		"for_each_task": map[string]any{
			"inputs": `["alpha","beta","gamma"]`,
			"task": map[string]any{
				"task_key": "inner",
				"spark_python_task": map[string]any{
					"python_file": "/jobs/each.py",
					"parameters":  []string{"--name", "{{input}}"},
				},
			},
		},
	}})
	out := runToCompletion(t, h, job)
	if out["state"].(map[string]any)["result_state"] != "SUCCESS" {
		t.Fatalf("loop run %+v", out)
	}
	// Each input ran, and each iteration saw ITS OWN value. Substituting into
	// the shared parsed task instead of a copy would leave every iteration
	// reading the last input — silent wrong-parameter processing, the exact
	// shape databricks#56 witnessed for sys.argv.
	var seen []string
	var mu sync.Mutex
	for _, c := range h.exec.Calls {
		for _, want := range []string{"alpha", "beta", "gamma"} {
			if strings.Contains(c.Code, want) {
				mu.Lock()
				seen = append(seen, want)
				mu.Unlock()
			}
		}
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		found := false
		for _, s := range seen {
			if s == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("input %q never reached the engine; saw %v across %d calls",
				want, seen, len(h.exec.Calls))
		}
	}
}

func TestForEachTaskFailsWhenAnIterationFails(t *testing.T) {
	h := newHarness(t)
	h.exec.Hook = func(spark.Request) (spark.Result, error) { return spark.Result{}, fmt.Errorf("boom") }
	_ = h.srv.Store.Workspace.Put("/jobs/each.py", []byte("print('x')\n"), "NOTEBOOK", "PYTHON")
	job := createJob(t, h, "loop", []map[string]any{{
		"task_key": "fan",
		"for_each_task": map[string]any{
			"inputs": `["a","b"]`,
			"task": map[string]any{
				"task_key":          "inner",
				"spark_python_task": map[string]any{"python_file": "/jobs/each.py"},
			},
		},
	}})
	out := runToCompletion(t, h, job)
	if out["state"].(map[string]any)["result_state"] != "FAILED" {
		t.Fatalf("a loop whose iterations all failed reported %+v", out["state"])
	}
}

func TestForEachTaskRefusesInputsThatRunNothing(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		name   string
		inputs any
		want   string
	}{
		{"empty list", `[]`, "runs nothing"},
		{"empty string", "", "required"},
		{"not JSON", "alpha,beta", "not a JSON array"},
		{"missing", nil, "required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"task": map[string]any{
				"task_key":          "inner",
				"spark_python_task": map[string]any{"python_file": "/jobs/each.py"},
			}}
			if tc.inputs != nil {
				body["inputs"] = tc.inputs
			}
			var created map[string]any
			st := h.json("POST", "/api/2.2/jobs/create", h.srv.Store.AdminPAT, map[string]any{
				"name":  "loop",
				"tasks": []map[string]any{{"task_key": "fan", "for_each_task": body}},
			}, &created)
			// Refused at CREATE, not at run: a loop over no inputs that reports
			// SUCCESS is the silent no-op this repo's refusals exist to prevent.
			if st == 200 {
				t.Fatalf("accepted inputs=%v, which would run nothing", tc.inputs)
			}
		})
	}
}

func TestForEachTaskRefusesNesting(t *testing.T) {
	h := newHarness(t)
	inner := map[string]any{
		"task_key": "inner",
		"for_each_task": map[string]any{
			"inputs": `["x"]`,
			"task":   map[string]any{"task_key": "innermost"},
		},
	}
	var created map[string]any
	st := h.json("POST", "/api/2.2/jobs/create", h.srv.Store.AdminPAT, map[string]any{
		"name": "loop",
		"tasks": []map[string]any{{
			"task_key":      "fan",
			"for_each_task": map[string]any{"inputs": `["a"]`, "task": inner},
		}},
	}, &created)
	if st == 200 {
		t.Fatalf("accepted a for_each_task nested inside a for_each_task")
	}
}
