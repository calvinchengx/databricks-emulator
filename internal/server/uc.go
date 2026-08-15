package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
)

func (s *Server) unityCatalog(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	if s.UC == nil || !s.UC.Attached() {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			"no Unity Catalog sidecar is attached — set DATABRICKS_UC_URL to a UC OSS server")
		return
	}
	if isUCGrantPath(r.URL.Path) {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			"Unity Catalog grants are not shipped until they deny")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if r.Method == http.MethodPost && isUCTablesCollection(r.URL.Path) && isManagedTable(body) {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			"MANAGED tables are refused: UC OSS only creates EXTERNAL tables at a filesystem location; inventing a managed table Spark cannot see is a lookalike")
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err := s.UC.Proxy(w, r); err != nil {
		writeError(w, http.StatusBadGateway, "ABORTED", err.Error())
	}
}

func isUCGrantPath(p string) bool {
	p = strings.ToLower(p)
	return strings.Contains(p, "/permissions/") || strings.HasSuffix(p, "/permissions") ||
		strings.Contains(p, "/grants/") || strings.HasSuffix(p, "/grants")
}

func isUCTablesCollection(p string) bool {
	p = strings.TrimSuffix(p, "/")
	return strings.HasSuffix(p, "/tables")
}

func isManagedTable(body []byte) bool {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	t, _ := raw["table_type"].(string)
	return strings.EqualFold(t, "MANAGED")
}
