package server

import (
	"testing"
)

// A job read back is the job that was created, edges included.
//
// `depends_on` was NOT serialized before the if/else work: a multi-task job
// created through the API came back from `jobs/get` with every task present
// and no edges at all. Nothing failed, and nothing in the run looked wrong;
// the DAG simply was not in the answer. A client that reconciles desired
// against actual state -- Terraform, and `databricks bundle deploy` -- reads
// that as drift it can never settle, because the edges it just wrote are
// missing from every read.
//
// Every task shape goes through one job here so the whole serializer is
// exercised by a round trip rather than by asserting the shape it happens to
// build today.
func TestJobRoundTripsEveryTaskShape(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	_ = h.srv.Store.Workspace.Put("/nb", []byte("x"), "NOTEBOOK", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/p.py", []byte("x"), "FILE", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/q.sql", []byte("select 1"), "FILE", "SQL")

	var created map[string]any
	if code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "every-shape",
		"tasks": []map[string]any{
			{"task_key": "cond", "condition_task": map[string]any{
				"op": "LESS_THAN", "left": "1", "right": "2"}},
			{"task_key": "nb",
				"depends_on":    []map[string]any{{"task_key": "cond", "outcome": "true"}},
				"notebook_task": map[string]any{"notebook_path": "/nb", "base_parameters": map[string]any{"k": "v"}}},
			{"task_key": "py",
				"depends_on":        []map[string]any{{"task_key": "cond", "outcome": "false"}},
				"spark_python_task": map[string]any{"python_file": "/p.py", "parameters": []any{"--a", "1"}}},
			{"task_key": "sq", "run_if": "ALL_DONE",
				"depends_on": []map[string]any{{"task_key": "nb"}, {"task_key": "py"}},
				"sql_task":   map[string]any{"file": map[string]any{"path": "/q.sql"}}},
		},
	}, &created); code != 200 {
		t.Fatalf("create = %d", code)
	}

	var got map[string]any
	if code := h.json("GET", "/api/2.2/jobs/get?job_id="+itoa(int64(created["job_id"].(float64))),
		pat, nil, &got); code != 200 {
		t.Fatalf("get = %d", code)
	}

	settings, ok := got["settings"].(map[string]any)
	if !ok {
		t.Fatalf("no settings in %v", got)
	}
	if settings["name"] != "every-shape" {
		t.Errorf("name = %v", settings["name"])
	}

	byKey := map[string]map[string]any{}
	for _, raw := range settings["tasks"].([]any) {
		m := raw.(map[string]any)
		byKey[m["task_key"].(string)] = m
	}
	if len(byKey) != 4 {
		t.Fatalf("tasks read back = %d, want 4", len(byKey))
	}

	// The edges survive, and so does the branch each one follows.
	deps := func(key string) []map[string]any {
		raw, ok := byKey[key]["depends_on"].([]any)
		if !ok {
			t.Fatalf("task %q came back with no depends_on; the DAG was dropped", key)
		}
		var out []map[string]any
		for _, d := range raw {
			out = append(out, d.(map[string]any))
		}
		return out
	}
	for _, tc := range []struct{ task, on, outcome string }{
		{"nb", "cond", "true"},
		{"py", "cond", "false"},
	} {
		d := deps(tc.task)
		if len(d) != 1 || d[0]["task_key"] != tc.on || d[0]["outcome"] != tc.outcome {
			t.Errorf("%s depends_on = %v, want [{%s %s}]", tc.task, d, tc.on, tc.outcome)
		}
	}
	// A plain edge carries no outcome rather than an empty one: a client that
	// round-trips this back must not turn an ordinary edge into a branch.
	sq := deps("sq")
	if len(sq) != 2 {
		t.Fatalf("sq depends_on = %v, want two edges", sq)
	}
	for _, d := range sq {
		if _, present := d["outcome"]; present {
			t.Errorf("plain edge carries an outcome key: %v", d)
		}
	}
	if byKey["sq"]["run_if"] != "ALL_DONE" {
		t.Errorf("run_if = %v, want ALL_DONE", byKey["sq"]["run_if"])
	}

	// Each task kind comes back as the kind it was created as.
	if c, ok := byKey["cond"]["condition_task"].(map[string]any); !ok {
		t.Error("condition_task missing")
	} else if c["op"] != "LESS_THAN" || c["left"] != "1" || c["right"] != "2" {
		t.Errorf("condition_task = %v", c)
	}
	if n, ok := byKey["nb"]["notebook_task"].(map[string]any); !ok {
		t.Error("notebook_task missing")
	} else if n["notebook_path"] != "/nb" {
		t.Errorf("notebook_task = %v", n)
	}
	if p, ok := byKey["py"]["spark_python_task"].(map[string]any); !ok {
		t.Error("spark_python_task missing")
	} else if p["python_file"] != "/p.py" {
		t.Errorf("spark_python_task = %v", p)
	}
	if s, ok := byKey["sq"]["sql_task"].(map[string]any); !ok {
		t.Error("sql_task missing")
	} else if s["file"].(map[string]any)["path"] != "/q.sql" {
		t.Errorf("sql_task = %v", s)
	}

	// A condition task is not also reported as something else, and an
	// ordinary task is not reported as a condition. Serializing by `if`
	// rather than by kind makes that a real risk.
	for key, field := range map[string]string{
		"cond": "notebook_task", "nb": "condition_task",
		"py": "condition_task", "sq": "notebook_task",
	} {
		if _, present := byKey[key][field]; present {
			t.Errorf("task %q also came back carrying %s", key, field)
		}
	}
}
