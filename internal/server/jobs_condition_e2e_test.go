package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

// An if/else job end to end, through the same REST the SDK drives.
//
// The witness is what reached the ENGINE, not what the run reported. A branch
// model that gates only the reported state would still send both arms to Spark:
// the work would be done and paid for, and the skip would be a label applied
// afterwards. `h.exec.Hook` records every task the engine actually saw.
func TestConditionRunsOnlyTheChosenBranchEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name       string
		left, op   string
		right      string
		wantRan    string // the only task key that may reach the engine
		wantSkip   string
		wantResult string
	}{
		{"true arm", "10", "GREATER_THAN", "5", "big", "small", "true"},
		{"false arm", "1", "GREATER_THAN", "5", "small", "big", "false"},
		// Databricks' documented example: string equality, so this is FALSE
		// and the false arm is the one that runs.
		{"equality is textual", "12.0", "EQUAL_TO", "12", "small", "big", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			pat := h.srv.Store.AdminPAT
			for _, n := range []string{"big", "small"} {
				_ = h.srv.Store.Workspace.Put("/"+n+".py", []byte(n), "FILE", "PYTHON")
			}

			var mu sync.Mutex
			var ran []string
			h.exec.Hook = func(req spark.Request) (spark.Result, error) {
				mu.Lock()
				ran = append(ran, strings.TrimPrefix(req.Session, "job-"))
				mu.Unlock()
				return spark.Result{OK: true, Stdout: "ok"}, nil
			}

			var created map[string]any
			h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
				"name": "branch",
				"tasks": []map[string]any{
					{"task_key": "cond", "condition_task": map[string]any{
						"op": tc.op, "left": tc.left, "right": tc.right,
					}},
					{"task_key": "big",
						"depends_on":        []map[string]any{{"task_key": "cond", "outcome": "true"}},
						"spark_python_task": map[string]any{"python_file": "/big.py"}},
					{"task_key": "small",
						"depends_on":        []map[string]any{{"task_key": "cond", "outcome": "false"}},
						"spark_python_task": map[string]any{"python_file": "/small.py"}},
				},
			}, &created)

			var run map[string]any
			h.json("POST", "/api/2.2/jobs/run-now", pat,
				map[string]any{"job_id": created["job_id"]}, &run)
			got := h.waitRun(int64(run["run_id"].(float64)))

			mu.Lock()
			reached := append([]string(nil), ran...)
			mu.Unlock()
			sort.Strings(reached)
			if len(reached) != 1 || reached[0] != tc.wantRan {
				t.Fatalf("tasks that reached the engine = %v, want only [%s]. "+
					"Both arms here would mean the unchosen branch was computed and "+
					"then relabelled, not skipped.", reached, tc.wantRan)
			}

			states := map[string]string{}
			outcome := ""
			for _, raw := range got["tasks"].([]any) {
				m := raw.(map[string]any)
				key := m["task_key"].(string)
				states[key] = m["state"].(map[string]any)["result_state"].(string)
				if ct, ok := m["condition_task"].(map[string]any); ok {
					outcome = ct["outcome"].(string)
				}
			}
			if outcome != tc.wantResult {
				t.Errorf("reported condition outcome = %q, want %q", outcome, tc.wantResult)
			}
			// A condition SUCCEEDS either way; the outcome above is the only
			// thing that says which arm was chosen.
			if states["cond"] != "SUCCESS" {
				t.Errorf("condition task result_state = %q, want SUCCESS", states["cond"])
			}
			if states[tc.wantSkip] != "SKIPPED" {
				t.Errorf("%s result_state = %q, want SKIPPED", tc.wantSkip, states[tc.wantSkip])
			}
			if states[tc.wantRan] != "SUCCESS" {
				t.Errorf("%s result_state = %q, want SUCCESS", tc.wantRan, states[tc.wantRan])
			}
			if got["state"].(map[string]any)["result_state"] != "SUCCESS" {
				t.Errorf("run result_state = %v, want SUCCESS", got["state"])
			}
		})
	}
}

// The three absent task types are refused BY NAME. They used to reach a
// generic "must be notebook_task, spark_python_task, or sql_task.file", which
// never told the caller that the thing they asked for is the thing missing.
func TestAbsentTaskTypesAreRefusedByName(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	for _, tc := range []struct{ field, want string }{
		{"python_wheel_task", "python_wheel_task"},
		{"run_job_task", "run_job_task"},
		{"for_each_task", "for_each_task"},
	} {
		body := map[string]any{"name": "x", "tasks": []map[string]any{
			{"task_key": "t", tc.field: map[string]any{}},
		}}
		var out map[string]any
		code := h.json("POST", "/api/2.2/jobs/create", pat, body, &out)
		if code == 200 {
			t.Fatalf("%s was accepted; it is not implemented", tc.field)
		}
		if !strings.Contains(fmt.Sprint(out), tc.want) {
			t.Errorf("%s refusal does not name it: %v", tc.field, out)
		}
	}
}

// An outcome that is neither branch is refused rather than silently treated as
// an ordinary edge, which would run a task the author meant to gate.
func TestUnknownOutcomeIsRefused(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "x", "tasks": []map[string]any{
			{"task_key": "c", "condition_task": map[string]any{
				"op": "EQUAL_TO", "left": "1", "right": "1"}},
			{"task_key": "t", "depends_on": []map[string]any{
				{"task_key": "c", "outcome": "maybe"}},
				"notebook_task": map[string]any{"notebook_path": "/n"}},
		},
	}, nil)
	if code == 200 {
		t.Fatal(`outcome "maybe" was accepted; only "true" and "false" are branches`)
	}
}

// An op outside the SDK's ConditionTaskOp enum is refused, not defaulted to
// one that happens to be implemented.
func TestUnknownConditionOpIsRefused(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "x", "tasks": []map[string]any{
			{"task_key": "c", "condition_task": map[string]any{
				"op": "CONTAINS", "left": "a", "right": "b"}},
		},
	}, nil)
	if code == 200 {
		t.Fatal("op CONTAINS was accepted; it is not a ConditionTaskOp")
	}
}
