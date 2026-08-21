package server

import (
	"fmt"
	"strings"
	"testing"
)

// spark_env_vars are refused on the task kinds that cannot carry them.
//
// They were accepted on every kind and delivered on three, so a run could
// report SUCCESS having never seen a variable it set. That is worse than a
// missing feature because `{{secrets/scope/key}}` resolves inside this field:
// the emulator really did fetch the secret, and then dropped it. A green run
// was compatible with the secret never arriving anywhere.
//
// The four kinds below are refused for reasons in `envUndeliverableOn`, and
// each is checked to NAME the kind, so a caller learns which of their tasks is
// the problem rather than that something somewhere was rejected.
func TestSparkEnvVarsRefusedWhereTheyCannotBeDelivered(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), "FILE", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/s.sql", []byte("select 1"), "FILE", "SQL")

	envVars := map[string]any{"spark_env_vars": map[string]any{"PW": "shhh"}}
	for _, tc := range []struct {
		name string
		task map[string]any
	}{
		{"sql_task", map[string]any{
			"sql_task": map[string]any{"file": map[string]any{"path": "/s.sql"}},
		}},
		{"condition_task", map[string]any{
			"condition_task": map[string]any{"op": "EQUAL_TO", "left": "1", "right": "1"},
		}},
		{"run_job_task", map[string]any{
			"run_job_task": map[string]any{"job_id": 1},
		}},
		{"for_each_task", map[string]any{
			"for_each_task": map[string]any{
				"inputs": `["a"]`,
				"task": map[string]any{
					"task_key":          "inner",
					"spark_python_task": map[string]any{"python_file": "/s.py"},
				},
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := map[string]any{"task_key": "t", "new_cluster": envVars}
			for k, v := range tc.task {
				task[k] = v
			}
			var out map[string]any
			st := h.json("POST", "/api/2.2/jobs/create", pat,
				map[string]any{"name": tc.name, "tasks": []map[string]any{task}}, &out)
			if st == 200 {
				t.Fatalf("spark_env_vars accepted on a %s; nothing would deliver them", tc.name)
			}
			if !strings.Contains(fmt.Sprint(out), tc.name) {
				t.Errorf("the refusal does not name the task kind: %v", out)
			}
			if !strings.Contains(fmt.Sprint(out), "spark_env_vars") {
				t.Errorf("the refusal does not name the field: %v", out)
			}
		})
	}
}

// And accepted, with the values decoded out of the statement, on the kinds
// that do carry them. Without this half the test above is satisfied by
// refusing everything.
func TestSparkEnvVarsReachTheKindsThatCarryThem(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), "FILE", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/nb.py", []byte("print(1)"), "NOTEBOOK", "PYTHON")

	for _, tc := range []struct {
		name string
		task map[string]any
	}{
		{"spark_python_task", map[string]any{
			"spark_python_task": map[string]any{"python_file": "/s.py"},
		}},
		{"notebook_task", map[string]any{
			"notebook_task": map[string]any{"notebook_path": "/nb.py"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.exec.Calls = nil
			task := map[string]any{
				"task_key":    "t",
				"new_cluster": map[string]any{"spark_env_vars": map[string]any{"PW": "shhh"}},
			}
			for k, v := range tc.task {
				task[k] = v
			}
			var created map[string]any
			if st := h.json("POST", "/api/2.2/jobs/create", pat,
				map[string]any{"name": tc.name, "tasks": []map[string]any{task}}, &created); st != 200 {
				t.Fatalf("create %d: %v", st, created)
			}
			var run map[string]any
			h.json("POST", "/api/2.2/jobs/run-now", pat,
				map[string]any{"job_id": created["job_id"]}, &run)
			h.waitRun(int64(run["run_id"].(float64)))
			if len(h.exec.Calls) == 0 {
				t.Fatal("the task never reached the engine, so this asserts nothing")
			}
			if got := deliveredEnv(t, h.exec.Calls[0].Code)["PW"]; got != "shhh" {
				t.Fatalf("a %s sees PW as %q", tc.name, got)
			}
		})
	}
}

// The request body carries no `env` and no `spark_conf`.
//
// Both were sent and neither was ever read: the agent's /statements handler
// takes `session`, `code`, `kind` and its identity fields, and `sparkConfig`
// is read only by /environment. Keeping them made the emulator LOOK like it
// delivered a task's environment over the wire, and gave tests a second place
// to assert against that could be green while the task saw nothing.
func TestTheStatementBodyCarriesNoEnvOrConf(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), "FILE", "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "wire",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "/s.py"},
			"new_cluster":       map[string]any{"spark_env_vars": map[string]any{"PW": "shhh"}},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.waitRun(int64(run["run_id"].(float64)))
	if len(h.exec.Calls) == 0 {
		t.Fatal("the task never reached the engine, so this asserts nothing")
	}
	// The value is in the STATEMENT, and the statement is the only place it
	// is. Asserting the body's shape directly belongs with the encoder
	// (internal/spark); what matters here is that nothing routes around the
	// preamble.
	if got := deliveredEnv(t, h.exec.Calls[0].Code)["PW"]; got != "shhh" {
		t.Fatalf("PW = %q; the preamble is the only delivery path", got)
	}
}
