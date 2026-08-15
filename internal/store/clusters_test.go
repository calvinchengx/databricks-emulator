package store

import "testing"

func TestClustersCRUDAndState(t *testing.T) {
	c := newClusters()
	cl := c.Create("dev", "emulator-spark", "emulator.session", 0, "admin")
	if cl.ID == "" || cl.State != "PENDING" {
		t.Fatalf("create %+v", cl)
	}
	got, ok := c.Get(cl.ID)
	if !ok || got.Name != "dev" || got.Creator != "admin" {
		t.Fatalf("get %+v %v", got, ok)
	}
	if len(c.List()) != 1 {
		t.Fatal("list")
	}
	if !c.SetState(cl.ID, "RUNNING", "session") || c.all[cl.ID].State != "RUNNING" {
		t.Fatal("running")
	}
	if c.SetState("missing", "RUNNING", "") {
		t.Fatal("missing state")
	}
	if !c.Delete(cl.ID) || c.Delete(cl.ID) {
		t.Fatal("delete")
	}
	if _, ok := c.Get(cl.ID); ok {
		t.Fatal("deleted still there")
	}
}
