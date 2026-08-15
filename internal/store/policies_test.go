package store

import (
	"strings"
	"testing"
)

func TestPolicyFixedEnforcedAndUnknownRefused(t *testing.T) {
	if err := ValidateDefinition(`{"dbus_per_hour":{"type":"range","maxValue":1}}`); err == nil || !strings.Contains(err.Error(), "not enforced") {
		t.Fatalf("unknown attr %v", err)
	}
	if err := ValidateDefinition(`{"node_type_id":{"type":"regex","pattern":".*"}}`); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("unknown type %v", err)
	}
	def := `{"node_type_id":{"type":"fixed","value":"emulator.session"},"num_workers":{"type":"range","minValue":0,"maxValue":0}}`
	if err := ValidateDefinition(def); err != nil {
		t.Fatal(err)
	}
	ok, err := EvaluatePolicy(def, ClusterAttrs{NodeTypeID: "emulator.session", NumWorkers: 0})
	if err != nil || len(ok) != 0 {
		t.Fatalf("compliant %+v %v", ok, err)
	}
	bad, err := EvaluatePolicy(def, ClusterAttrs{NodeTypeID: "i3.xlarge", NumWorkers: 2})
	if err != nil || bad["node_type_id"] == "" || bad["num_workers"] == "" {
		t.Fatalf("violations %+v %v", bad, err)
	}
	forbid := `{"autoscale":{"type":"forbidden"},"libraries":{"type":"forbidden"},"spark_version":{"type":"allowlist","values":["emulator-spark"]},"node_type_id":{"type":"unlimited"}}`
	if err := ValidateDefinition(forbid); err != nil {
		t.Fatal(err)
	}
	if v, err := EvaluatePolicy(forbid, ClusterAttrs{SparkVersion: "emulator-spark", Autoscale: true, Libraries: true}); err != nil || v["autoscale"] == "" || v["libraries"] == "" {
		t.Fatalf("forbidden %+v %v", v, err)
	}
	if v, err := EvaluatePolicy(forbid, ClusterAttrs{SparkVersion: "emulator-spark"}); err != nil || len(v) != 0 {
		t.Fatalf("allowlist %+v %v", v, err)
	}
}

func TestPoliciesPersist(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	pol, err := s.Policies.CreatePolicy("session", `{"node_type_id":{"type":"fixed","value":"emulator.session"}}`, "", "", "admin", 1)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Policies.Get(pol.ID)
	if !ok || got.Name != "session" {
		t.Fatalf("reload %+v ok=%v", got, ok)
	}
	if len(s2.Policies.List()) != 1 {
		t.Fatal("list")
	}
	renamed, err := s2.Policies.Edit(pol.ID, "renamed", "", "desc")
	if err != nil || renamed.Name != "renamed" {
		t.Fatalf("edit %+v %v", renamed, err)
	}
	if !s2.Policies.Delete(pol.ID) {
		t.Fatal("delete")
	}
}
