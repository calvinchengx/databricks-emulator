package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/sqlshim"
	"github.com/calvinchengx/databricks-emulator/internal/store"
	"github.com/calvinchengx/databricks-emulator/internal/uc"
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
	plan := sqlshim.Rewrite(st.SQL, s.Cfg.DeltaRoot)
	if plan.Err != "" {
		// The rewrite refused: fail by name rather than letting the engine
		// parse something assembled from an unsafe identifier.
		st.Status = "FAILED"
		st.Error = plan.Err
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	if plan.CreateSchema != nil {
		if err := s.ensureUCSchema(plan.CreateSchema); err != nil {
			st.Status = "FAILED"
			st.Error = err.Error()
			s.Store.SQL.UpdateStatement(st)
			s.recordStatementHistory(st, started)
			return
		}
		st.Status = "SUCCEEDED"
		st.Stdout = "[]"
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	if plan.SkipEngine && plan.EmptyJSON {
		st.Status = "SUCCEEDED"
		st.Stdout = sqlshim.EmptyInformationSchema
		s.Store.SQL.UpdateStatement(st)
		s.recordStatementHistory(st, started)
		return
	}
	engineSQL := plan.SQL
	res, err := s.Spark.Run(sparkSQLRequest(engineSQL, "sql-"+st.ID))
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
	if plan.Register != nil {
		if err := s.registerExternalTable(plan.Register, st.ID); err != nil {
			st.Status = "FAILED"
			st.Error = err.Error()
			s.Store.SQL.UpdateStatement(st)
			s.recordStatementHistory(st, started)
			return
		}
	}
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

func (s *Server) ensureUCSchema(ref *sqlshim.SchemaRef) error {
	if s.UC == nil || !s.UC.Attached() {
		return fmt.Errorf("CREATE SCHEMA %s.%s needs DATABRICKS_UC_URL", ref.Catalog, ref.Schema)
	}
	if ref.Catalog == "" || ref.Schema == "" {
		return fmt.Errorf("CREATE SCHEMA needs catalog.schema")
	}
	st, body, err := s.UC.JSON("POST", "/api/2.1/unity-catalog/schemas", map[string]string{
		"name": ref.Schema, "catalog_name": ref.Catalog,
	})
	if err != nil {
		return err
	}
	if st >= 300 && !uc.AlreadyThere(st, body) {
		return fmt.Errorf("unity catalog schema: status %d: %s", st, body)
	}
	return nil
}

func (s *Server) registerExternalTable(t *sqlshim.ExternalTable, stmtID string) error {
	if s.UC == nil || !s.UC.Attached() {
		return nil
	}
	cols := s.describeForUC(t.Name, stmtID)
	if len(cols) == 0 {
		cols = []map[string]any{{
			"name":      "_col",
			"type_name": "STRING",
			"type_text": "string",
			"type_json": `{"name":"_col","type":"string","nullable":true,"metadata":{}}`,
			"position":  0,
			"nullable":  true,
		}}
	}
	st, body, err := s.UC.JSON("POST", "/api/2.1/unity-catalog/tables", map[string]any{
		"name":               t.Name,
		"catalog_name":       t.Catalog,
		"schema_name":        t.Schema,
		"table_type":         "EXTERNAL",
		"data_source_format": "DELTA",
		"storage_location":   t.Location,
		"columns":            cols,
	})
	if err != nil {
		return err
	}
	if st >= 300 && !uc.AlreadyThere(st, body) {
		return fmt.Errorf("unity catalog table: status %d: %s", st, body)
	}
	return nil
}

func (s *Server) describeForUC(name, stmtID string) []map[string]any {
	// Same Spark session as the CREATE: Sail's memory catalog is session-scoped.
	res, err := s.Spark.Run(sparkSQLRequest("DESCRIBE TABLE `"+name+"`", "sql-"+stmtID))
	if err != nil || !res.OK {
		return nil
	}
	return sqlshim.ColumnsFromDescribe(res.Stdout)
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
