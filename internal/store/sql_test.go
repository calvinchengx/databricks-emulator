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
	gotRun, ok := s.FirstRunning()
	if !ok || gotRun.ID != w.ID {
		t.Fatalf("first running %+v %v", gotRun, ok)
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
	if _, ok := s.FirstRunning(); ok {
		t.Fatal("stopped still running")
	}
}

func TestSQLQueryCRUDAndHistoryFilter(t *testing.T) {
	s := newSQL()
	w := s.CreateWarehouse("wh", "2X-Small")
	name, ok := s.ResolveDisplayName("one", false, "")
	if !ok || name != "one" {
		t.Fatalf("resolve %+v %v", name, ok)
	}
	q := s.CreateQuery(&Query{DisplayName: "one", QueryText: "SELECT 1", WarehouseID: w.ID})
	if q.ID == "" || q.LifecycleState != "ACTIVE" || q.RunAsMode != "OWNER" {
		t.Fatalf("create %+v", q)
	}
	if _, ok := s.ResolveDisplayName("one", false, ""); ok {
		t.Fatal("duplicate name allowed")
	}
	resolved, ok := s.ResolveDisplayName("one", true, "")
	if !ok || resolved != "one (1)" {
		t.Fatalf("auto resolve %q %v", resolved, ok)
	}
	if !s.DisplayNameTaken("one", "") || s.DisplayNameTaken("one", q.ID) {
		t.Fatal("taken except self")
	}
	got, ok := s.GetQuery(q.ID)
	if !ok || got.QueryText != "SELECT 1" {
		t.Fatalf("get %+v %v", got, ok)
	}
	if len(s.ListQueries()) != 1 {
		t.Fatal("list")
	}
	q.QueryText = "SELECT 2"
	s.UpdateQuery(q)
	if got, _ := s.GetQuery(q.ID); got.QueryText != "SELECT 2" {
		t.Fatal("update")
	}
	if !s.TrashQuery(q.ID) || s.TrashQuery(q.ID) {
		t.Fatal("trash")
	}
	if len(s.ListQueries()) != 0 {
		t.Fatal("trashed still listed")
	}
	if _, ok := s.GetQuery(q.ID); !ok {
		t.Fatal("trashed get")
	}

	s.RecordHistory(&QueryHistory{QueryID: "stmt-1", WarehouseID: w.ID, QueryText: "SELECT 1", Status: "FINISHED", QueryStartTimeMs: 1000})
	s.RecordHistory(&QueryHistory{QueryID: "stmt-2", WarehouseID: w.ID, QueryText: "SELECT 2", Status: "FAILED", QueryStartTimeMs: 2000})
	s.RecordHistory(&QueryHistory{QueryID: "stmt-3", WarehouseID: "other", QueryText: "SELECT 3", Status: "FINISHED", QueryStartTimeMs: 3000})
	page, next, has := s.ListHistory(HistoryFilter{WarehouseIDs: []string{w.ID}}, 0, 1)
	if len(page) != 1 || page[0].QueryID != "stmt-2" || !has || next != 1 {
		t.Fatalf("page %+v next=%d has=%v", page, next, has)
	}
	failed, _, has := s.ListHistory(HistoryFilter{Statuses: []string{"FAILED"}}, 0, 10)
	if len(failed) != 1 || failed[0].QueryID != "stmt-2" || has {
		t.Fatalf("status %+v has=%v", failed, has)
	}
	one, _, _ := s.ListHistory(HistoryFilter{StatementIDs: []string{"stmt-1"}}, 0, 10)
	if len(one) != 1 || one[0].QueryText != "SELECT 1" {
		t.Fatalf("statement id %+v", one)
	}
	ranged, _, _ := s.ListHistory(HistoryFilter{StartTimeMs: 1500, EndTimeMs: 2500}, 0, 10)
	if len(ranged) != 1 || ranged[0].QueryID != "stmt-2" {
		t.Fatalf("range %+v", ranged)
	}
	if _, ok := s.GetQuery("missing"); ok {
		t.Fatal("missing query")
	}
}
