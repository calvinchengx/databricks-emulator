package server

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
)

func (s *Server) sqlThrift(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	s.HS2.ServeHTTP(w, r, id)
}

func (s *Server) Lookup(id string) (running bool, ok bool) {
	wh, ok := s.Store.SQL.GetWarehouse(id)
	if !ok {
		return false, false
	}
	return wh.State == "RUNNING", true
}

func (s *Server) Run(warehouseID, sql string) (string, error) {
	wh, ok := s.Store.SQL.GetWarehouse(warehouseID)
	if !ok {
		return "", fmt.Errorf("warehouse not found")
	}
	st := s.Store.SQL.NewStatement(warehouseID, sql)
	s.runSQLStatement(st, wh)
	if st.Status != "SUCCEEDED" {
		if st.Error == "" {
			return "", errors.New(st.Status)
		}
		return "", errors.New(st.Error)
	}
	return st.Stdout, nil
}
