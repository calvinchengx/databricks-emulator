package store

import "testing"

func TestSQLWarehouseAndStatementCRUD(t *testing.T) {
	s := newSQL()
	w := s.CreateWarehouse("starter", "2X-Small")
	if w.ID == "" || w.State != "RUNNING" || w.ClusterSize != "2X-Small" {
		t.Fatalf("create %+v", w)
	}
	got, ok := s.GetWarehouse(w.ID)
	if !ok || got.Name != "starter" {
		t.Fatalf("get %+v %v", got, ok)
	}
	if len(s.ListWarehouses()) != 1 {
		t.Fatal("list")
	}
	if !s.SetWarehouseState(w.ID, "STOPPED") || s.wh[w.ID].State != "STOPPED" {
		t.Fatal("stop")
	}
	st := s.NewStatement(w.ID, "SELECT 1")
	if st.Dialect != "spark-sql" || st.Status != "PENDING" {
		t.Fatalf("stmt %+v", st)
	}
	st.Status = "SUCCEEDED"
	s.UpdateStatement(st)
	gotSt, ok := s.GetStatement(st.ID)
	if !ok || gotSt.Status != "SUCCEEDED" {
		t.Fatalf("get stmt %+v %v", gotSt, ok)
	}
	if !s.DeleteWarehouse(w.ID) || s.DeleteWarehouse(w.ID) {
		t.Fatal("delete")
	}
	if _, ok := s.GetWarehouse(w.ID); ok {
		t.Fatal("deleted still there")
	}
	if s.SetWarehouseState("missing", "RUNNING") {
		t.Fatal("missing state")
	}
}
