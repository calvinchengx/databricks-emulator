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
	// NO COLUMN METADATA, DELIBERATELY. The Delta log at `storage_location`
	// already holds the authoritative schema, and Sail reads it from there.
	// Registering a second copy in UC is what created issue #46: this used to
	// DESCRIBE the table and map each type into a UC column, and decimal(p,s)
	// has no representation it could use --
	//
	//   decimal(p,s) in UC   -> Sail refuses the read outright
	//                           ("Unsupported complex type: decimal(19,4)")
	//   DOUBLE in UC         -> Sail reads, and BINDS THE COLUMN AS DOUBLE
	//
	// So the old workaround advertised DOUBLE. The release note said the Delta
	// files still held the engine type, and for the registered table that was
	// true -- but it is not where the damage lands. Anything reading that
	// table through UC got a double, so every DERIVED table was written
	// physically double: measured on a gold star, `fct_sales.amount_usd` is
	// decimal(19,4) in its own Delta log and `typeof()` answers `double`, and
	// the summary built from it holds double on disk. A money column had
	// stopped being money, and no row check can see that.
	//
	// Omitting columns is what the tables that were never described already
	// did, and they are the ones that behaved: a UC entry with no columns
	// reads back decimal(19,4) from the same bytes. The trade is explicit --
	// UC serves less metadata than real Databricks, in exchange for types that
	// are correct. Wrong types change query results; absent metadata does not,
	// and nothing here reads it (information_schema.columns is empty either
	// way). Restore the description when Sail's unity provider can express
	// decimal.
	//
	// A `_col` STRING placeholder used to stand in when DESCRIBE failed. It is
	// gone for the same reason and one worse: UC accepts an empty column list,
	// and a table registered with the placeholder is UNREADABLE -- Sail binds
	// only `_col`, so every real column "is missing from the schema".
	st, body, err := s.UC.JSON("POST", "/api/2.1/unity-catalog/tables", map[string]any{
		"name":               t.Name,
		"catalog_name":       t.Catalog,
		"schema_name":        t.Schema,
		"table_type":         "EXTERNAL",
		"data_source_format": "DELTA",
		"storage_location":   t.Location,
		"columns":            []map[string]any{},
	})
	if err != nil {
		return err
	}
	if st >= 300 && !uc.AlreadyThere(st, body) {
		return fmt.Errorf("unity catalog table: status %d: %s", st, body)
	}
	return nil
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
