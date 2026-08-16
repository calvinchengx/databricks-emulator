package server

import (
	"encoding/json"
	"net/http"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) sqlCreateWarehouse(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Name        string `json:"name"`
		ClusterSize string `json:"cluster_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "name is required")
		return
	}
	wh := s.Store.SQL.CreateWarehouse(body.Name, body.ClusterSize)
	writeJSON(w, http.StatusOK, warehouseJSON(wh))
}

func (s *Server) sqlListWarehouses(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	var list []map[string]any
	for _, wh := range s.Store.SQL.ListWarehouses() {
		list = append(list, warehouseJSON(wh))
	}
	writeJSON(w, http.StatusOK, map[string]any{"warehouses": list})
}

func (s *Server) sqlGetWarehouse(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := r.PathValue("id")
	wh, ok := s.Store.SQL.GetWarehouse(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
		return
	}
	writeJSON(w, http.StatusOK, warehouseJSON(wh))
}

func (s *Server) sqlDeleteWarehouse(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := r.PathValue("id")
	if !s.Store.SQL.DeleteWarehouse(id) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) sqlStartWarehouse(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := r.PathValue("id")
	if !s.Store.SQL.SetWarehouseState(id, "RUNNING") {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) sqlStopWarehouse(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := r.PathValue("id")
	if !s.Store.SQL.SetWarehouseState(id, "STOPPED") {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) sqlExecuteStatement(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		WarehouseID string `json:"warehouse_id"`
		Statement   string `json:"statement"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if body.Statement == "" || body.WarehouseID == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "warehouse_id and statement are required")
		return
	}
	wh, ok := s.Store.SQL.GetWarehouse(body.WarehouseID)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
		return
	}
	st := s.Store.SQL.NewStatement(body.WarehouseID, body.Statement)
	st.UserName = p.UserName
	s.runSQLStatement(st, wh)
	writeJSON(w, http.StatusOK, statementJSON(st))
}

func (s *Server) sqlGetStatement(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := r.PathValue("id")
	st, ok := s.Store.SQL.GetStatement(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "statement not found")
		return
	}
	writeJSON(w, http.StatusOK, statementJSON(st))
}

func (s *Server) runSQLStatement(st *store.Statement, wh *store.Warehouse) {
	started := s.Clock.Now()
	if wh.State != "RUNNING" {
		st.Status = "FAILED"
		st.Error = "warehouse is STOPPED"
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	if s.Spark == nil {
		st.Status = "FAILED"
		st.Error = "no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL"
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	res, err := s.Spark.Run(sparkSQLRequest(st.SQL, "sql-"+st.ID))
	if err != nil {
		st.Status = "FAILED"
		st.Error = err.Error()
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	st.Stdout = res.Stdout
	if !res.OK {
		st.Status = "FAILED"
		st.Error = res.EValue
		if st.Error == "" {
			st.Error = res.EName
		}
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	st.Status = "SUCCEEDED"
	s.Store.SQL.UpdateStatement(st)
	s.recordStatementHistory(st, started)
}

func sparkSQLRequest(sql, session string) spark.Request {
	return spark.Request{
		Session: session,
		Kind:    "sql",
		Code:    sql,
	}
}

func warehouseJSON(wh *store.Warehouse) map[string]any {
	return map[string]any{
		"id":           wh.ID,
		"name":         wh.Name,
		"state":        wh.State,
		"cluster_size": wh.ClusterSize,
	}
}

func statementJSON(st *store.Statement) map[string]any {
	out := map[string]any{
		"statement_id": st.ID,
		"status":       map[string]any{"state": st.Status},
		"dialect":      st.Dialect,
		"executedBy":   st.ExecutedBy,
	}
	if st.Error != "" {
		out["status"] = map[string]any{"state": st.Status, "error": map[string]any{"message": st.Error}}
	}
	if st.Status == "SUCCEEDED" {
		out["manifest"] = map[string]any{"format": "JSON_ARRAY"}
		out["result"] = map[string]any{"data_array": []any{}, "row_count": 0}
		if st.Stdout != "" {
			out["result"] = map[string]any{"text": st.Stdout}
		}
	}
	return out
}
