package server

import (
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestClusterPoliciesEnforceAndFamilies(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	h.srv.Spark = nil

	if st := h.json("GET", "/api/2.0/policies/clusters/list", "", nil, nil); st != 401 {
		t.Fatalf("unauth %d", st)
	}

	var created map[string]any
	def := `{"node_type_id":{"type":"fixed","value":"emulator.session"},"spark_version":{"type":"fixed","value":"emulator-spark"}}`
	if st := h.json("POST", "/api/2.0/policies/clusters/create", pat, map[string]any{
		"name": "session-only", "definition": def,
	}, &created); st != 200 {
		t.Fatalf("create %d %+v", st, created)
	}
	id := str(created["policy_id"])
	if id == "" {
		t.Fatal("no policy_id")
	}

	var body map[string]any
	if st := h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{
		"cluster_name": "bad", "policy_id": id, "node_type_id": "i3.xlarge",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "node_type_id") {
		t.Fatalf("mismatch %d %+v", st, body)
	}

	if st := h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{
		"cluster_name": "ok", "policy_id": id, "node_type_id": "emulator.session",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("match still needs engine %d %+v", st, body)
	}

	if st := h.json("POST", "/api/2.0/policies/clusters/create", pat, map[string]any{
		"name": "dbus", "definition": `{"dbus_per_hour":{"type":"range","maxValue":1}}`,
	}, &body); st != 501 || !strings.Contains(str(body["message"]), "not enforced") {
		t.Fatalf("unknown attr %d %+v", st, body)
	}

	var families map[string]any
	if st := h.json("GET", "/api/2.0/policy-families", pat, nil, &families); st != 200 {
		t.Fatalf("families %d", st)
	}
	list, _ := families["policy_families"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["policy_family_id"] != "emulator-session" {
		t.Fatalf("families %+v", families)
	}

	var fromFamily map[string]any
	if st := h.json("POST", "/api/2.0/policies/clusters/create", pat, map[string]any{
		"name": "from-family", "policy_family_id": "emulator-session",
	}, &fromFamily); st != 200 {
		t.Fatalf("from family %d %+v", st, fromFamily)
	}
	fid := str(fromFamily["policy_id"])
	var got map[string]any
	if st := h.json("GET", "/api/2.0/policies/clusters/get?policy_id="+fid, pat, nil, &got); st != 200 {
		t.Fatalf("get %d", st)
	}
	if !strings.Contains(str(got["definition"]), "emulator.session") {
		t.Fatalf("family definition %+v", got)
	}
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/policies/clusters/list", pat, nil, &listed); st != 200 {
		t.Fatalf("list %d", st)
	}
	if len(listed["policies"].([]any)) < 2 {
		t.Fatalf("list %+v", listed)
	}
	if st := h.json("POST", "/api/2.0/policies/clusters/edit", pat, map[string]any{
		"policy_id": id, "description": "session handle only",
	}, &got); st != 200 || str(got["description"]) != "session handle only" {
		t.Fatalf("edit %d %+v", st, got)
	}

	// Compliance needs a stored handle. Scripted engine is nil here; create a
	// matching cluster by temporarily attaching the harness executor.
	h.srv.Spark = h.exec
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: true, Stdout: "1"}, nil
	}
	var cl map[string]any
	if st := h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{
		"cluster_name": "comply", "policy_id": id,
	}, &cl); st != 200 {
		t.Fatalf("create with engine %d %+v", st, cl)
	}
	var comp map[string]any
	if st := h.json("GET", "/api/2.0/policies/clusters/get-compliance?cluster_id="+str(cl["cluster_id"]), pat, nil, &comp); st != 200 {
		t.Fatalf("compliance %d", st)
	}
	if comp["is_compliant"] != true {
		t.Fatalf("compliance %+v", comp)
	}
	if st := h.json("POST", "/api/2.0/clusters/delete", pat, map[string]any{"cluster_id": str(cl["cluster_id"])}, nil); st != 200 {
		t.Fatalf("cluster delete %d", st)
	}
	if st := h.json("POST", "/api/2.0/policies/clusters/delete", pat, map[string]any{"policy_id": id}, nil); st != 200 {
		t.Fatalf("policy delete %d", st)
	}
}
