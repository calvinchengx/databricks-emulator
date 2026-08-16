package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

type queryBody struct {
	ApplyAutoLimit *bool    `json:"apply_auto_limit"`
	Catalog        string   `json:"catalog"`
	Description    string   `json:"description"`
	DisplayName    string   `json:"display_name"`
	Parameters     []any    `json:"parameters"`
	ParentPath     string   `json:"parent_path"`
	QueryText      string   `json:"query_text"`
	RunAsMode      string   `json:"run_as_mode"`
	Schema         string   `json:"schema"`
	Tags           []string `json:"tags"`
	WarehouseID    string   `json:"warehouse_id"`
}

func (s *Server) sqlCreateQuery(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		AutoResolveDisplayName bool      `json:"auto_resolve_display_name"`
		Query                  queryBody `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	q := body.Query
	if q.DisplayName == "" || q.QueryText == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "query.display_name and query.query_text are required")
		return
	}
	if q.WarehouseID != "" {
		if _, ok := s.Store.SQL.GetWarehouse(q.WarehouseID); !ok {
			writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
			return
		}
	}
	name, ok := s.Store.SQL.ResolveDisplayName(q.DisplayName, body.AutoResolveDisplayName, "")
	if !ok {
		writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", "display_name already exists")
		return
	}
	now := s.rfc3339()
	saved := s.Store.SQL.CreateQuery(&store.Query{
		DisplayName:          name,
		QueryText:            q.QueryText,
		WarehouseID:          q.WarehouseID,
		Description:          q.Description,
		Catalog:              q.Catalog,
		Schema:               q.Schema,
		ParentPath:           q.ParentPath,
		Tags:                 append([]string(nil), q.Tags...),
		Parameters:           q.Parameters,
		ApplyAutoLimit:       q.ApplyAutoLimit != nil && *q.ApplyAutoLimit,
		RunAsMode:            q.RunAsMode,
		CreateTime:           now,
		UpdateTime:           now,
		OwnerUserName:        p.UserName,
		LastModifierUserName: p.UserName,
	})
	writeJSON(w, http.StatusOK, queryJSON(saved))
}

func (s *Server) sqlListQueries(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	all := s.Store.SQL.ListQueries()
	pageSize := atoiOr(r.URL.Query().Get("page_size"), 100)
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	offset := atoiOr(r.URL.Query().Get("page_token"), 0)
	if offset < 0 {
		offset = 0
	}
	if offset > len(all) {
		offset = len(all)
	}
	end := offset + pageSize
	if end > len(all) {
		end = len(all)
	}
	results := []map[string]any{}
	for _, q := range all[offset:end] {
		results = append(results, queryJSON(q))
	}
	out := map[string]any{"results": results}
	if end < len(all) {
		out["next_page_token"] = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) sqlGetQuery(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	q, ok := s.Store.SQL.GetQuery(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "query not found")
		return
	}
	writeJSON(w, http.StatusOK, queryJSON(q))
}

func (s *Server) sqlUpdateQuery(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		AutoResolveDisplayName bool      `json:"auto_resolve_display_name"`
		UpdateMask             string    `json:"update_mask"`
		Query                  queryBody `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if strings.TrimSpace(body.UpdateMask) == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "update_mask is required")
		return
	}
	q, ok := s.Store.SQL.GetQuery(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "query not found")
		return
	}
	if q.LifecycleState == "TRASHED" {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "query not found")
		return
	}
	fields := updateMaskFields(body.UpdateMask)
	if _, apply := fields["display_name"]; apply || fields["*"] {
		name, ok := s.Store.SQL.ResolveDisplayName(body.Query.DisplayName, body.AutoResolveDisplayName, q.ID)
		if !ok {
			writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", "display_name already exists")
			return
		}
		q.DisplayName = name
	}
	if _, apply := fields["query_text"]; apply || fields["*"] {
		q.QueryText = body.Query.QueryText
	}
	if _, apply := fields["warehouse_id"]; apply || fields["*"] {
		if body.Query.WarehouseID != "" {
			if _, ok := s.Store.SQL.GetWarehouse(body.Query.WarehouseID); !ok {
				writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "warehouse not found")
				return
			}
		}
		q.WarehouseID = body.Query.WarehouseID
	}
	if _, apply := fields["description"]; apply || fields["*"] {
		q.Description = body.Query.Description
	}
	if _, apply := fields["catalog"]; apply || fields["*"] {
		q.Catalog = body.Query.Catalog
	}
	if _, apply := fields["schema"]; apply || fields["*"] {
		q.Schema = body.Query.Schema
	}
	if _, apply := fields["parent_path"]; apply || fields["*"] {
		q.ParentPath = body.Query.ParentPath
	}
	if _, apply := fields["tags"]; apply || fields["*"] {
		q.Tags = append([]string(nil), body.Query.Tags...)
	}
	if _, apply := fields["parameters"]; apply || fields["*"] {
		q.Parameters = body.Query.Parameters
	}
	if _, apply := fields["apply_auto_limit"]; apply || fields["*"] {
		q.ApplyAutoLimit = body.Query.ApplyAutoLimit != nil && *body.Query.ApplyAutoLimit
	}
	if _, apply := fields["run_as_mode"]; apply || fields["*"] {
		q.RunAsMode = body.Query.RunAsMode
	}
	q.UpdateTime = s.rfc3339()
	q.LastModifierUserName = p.UserName
	s.Store.SQL.UpdateQuery(q)
	writeJSON(w, http.StatusOK, queryJSON(q))
}

func (s *Server) sqlDeleteQuery(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	if !s.Store.SQL.TrashQuery(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "query not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) sqlQueryVisualizationsRefused(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
		"query visualizations are refused — no dashboard renderer is attached")
}

func (s *Server) sqlAlertsRefused(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
		"SQL alerts are refused — no alert evaluator is attached")
}

func (s *Server) sqlListQueryHistory(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	q := r.URL.Query()
	limit := atoiOr(q.Get("max_results"), 100)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := atoiOr(q.Get("page_token"), 0)
	if offset < 0 {
		offset = 0
	}
	startMs, _ := strconv.ParseInt(q.Get("filter_by.query_start_time_range.start_time_ms"), 10, 64)
	endMs, _ := strconv.ParseInt(q.Get("filter_by.query_start_time_range.end_time_ms"), 10, 64)
	items, next, hasNext := s.Store.SQL.ListHistory(store.HistoryFilter{
		WarehouseIDs: q["filter_by.warehouse_ids"],
		Statuses:     q["filter_by.statuses"],
		StatementIDs: q["filter_by.statement_ids"],
		StartTimeMs:  startMs,
		EndTimeMs:    endMs,
	}, offset, limit)
	res := []map[string]any{}
	for _, h := range items {
		res = append(res, historyJSON(h))
	}
	out := map[string]any{"res": res, "has_next_page": hasNext}
	if hasNext {
		out["next_page_token"] = strconv.Itoa(next)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) recordStatementHistory(st *store.Statement, started int64) {
	status := "FAILED"
	if st.Status == "SUCCEEDED" {
		status = "FINISHED"
	}
	end := s.Clock.Now()
	s.Store.SQL.RecordHistory(&store.QueryHistory{
		QueryID:            st.ID,
		WarehouseID:        st.WarehouseID,
		QueryText:          st.SQL,
		Status:             status,
		ErrorMessage:       st.Error,
		QueryStartTimeMs:   started * 1000,
		QueryEndTimeMs:     end * 1000,
		ExecutionEndTimeMs: end * 1000,
		Duration:           (end - started) * 1000,
		IsFinal:            true,
		UserName:           st.UserName,
		StatementType:      statementType(st.SQL),
	})
}

func (s *Server) rfc3339() string {
	return time.Unix(s.Clock.Now(), 0).UTC().Format(time.RFC3339)
}

func queryJSON(q *store.Query) map[string]any {
	out := map[string]any{
		"id":                      q.ID,
		"display_name":            q.DisplayName,
		"query_text":              q.QueryText,
		"lifecycle_state":         q.LifecycleState,
		"run_as_mode":             q.RunAsMode,
		"apply_auto_limit":        q.ApplyAutoLimit,
		"create_time":             q.CreateTime,
		"update_time":             q.UpdateTime,
		"owner_user_name":         q.OwnerUserName,
		"last_modifier_user_name": q.LastModifierUserName,
	}
	if q.WarehouseID != "" {
		out["warehouse_id"] = q.WarehouseID
	}
	if q.Description != "" {
		out["description"] = q.Description
	}
	if q.Catalog != "" {
		out["catalog"] = q.Catalog
	}
	if q.Schema != "" {
		out["schema"] = q.Schema
	}
	if q.ParentPath != "" {
		out["parent_path"] = q.ParentPath
	}
	if len(q.Tags) > 0 {
		out["tags"] = q.Tags
	}
	if len(q.Parameters) > 0 {
		out["parameters"] = q.Parameters
	}
	return out
}

func historyJSON(h *store.QueryHistory) map[string]any {
	out := map[string]any{
		"query_id":              h.QueryID,
		"query_text":            h.QueryText,
		"status":                h.Status,
		"is_final":              h.IsFinal,
		"query_start_time_ms":   h.QueryStartTimeMs,
		"query_end_time_ms":     h.QueryEndTimeMs,
		"execution_end_time_ms": h.ExecutionEndTimeMs,
		"duration":              h.Duration,
		"statement_type":        h.StatementType,
	}
	if h.WarehouseID != "" {
		out["warehouse_id"] = h.WarehouseID
	}
	if h.ErrorMessage != "" {
		out["error_message"] = h.ErrorMessage
	}
	if h.UserName != "" {
		out["user_name"] = h.UserName
	}
	return out
}

func updateMaskFields(mask string) map[string]bool {
	out := map[string]bool{}
	for _, f := range strings.Split(mask, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			out[f] = true
		}
	}
	return out
}

func atoiOr(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return n
}

func statementType(sql string) string {
	word := firstSQLWord(sql)
	switch word {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP",
		"ALTER", "SHOW", "DESCRIBE", "EXPLAIN", "OPTIMIZE", "ANALYZE",
		"TRUNCATE", "SET", "USE", "GRANT", "REVOKE", "CALL", "COPY",
		"REFRESH", "REPLACE":
		return word
	default:
		return "OTHER"
	}
}

func firstSQLWord(sql string) string {
	s := strings.TrimSpace(sql)
	i := 0
	for i < len(s) && (unicode.IsLetter(rune(s[i])) || s[i] == '_') {
		i++
	}
	return strings.ToUpper(s[:i])
}
