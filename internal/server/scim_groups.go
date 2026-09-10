package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

// SCIM group endpoints. The wire shape is SCIM v2 because that is what an
// unmodified databricks-sdk sends and parses; anything else would pass a
// hand-written client and fail a real one.

type scimMember struct {
	Value string `json:"value"`
}

func groupJSON(g *store.Group) map[string]any {
	members := make([]map[string]any, 0, len(g.Members))
	for _, m := range g.Members {
		members = append(members, map[string]any{"value": m})
	}
	return map[string]any{
		"schemas":     []string{"urn:ietf:params:scim:schemas:core:2.0:Group"},
		"id":          g.ID,
		"displayName": g.DisplayName,
		"members":     members,
		"meta":        map[string]any{"resourceType": "Group"},
	}
}

func (s *Server) scimGroupsCreate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		DisplayName string       `json:"displayName"`
		Members     []scimMember `json:"members"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "MALFORMED_REQUEST", "invalid SCIM group payload")
		return
	}
	if strings.TrimSpace(body.DisplayName) == "" {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "displayName is required")
		return
	}
	members := make([]string, 0, len(body.Members))
	for _, m := range body.Members {
		members = append(members, m.Value)
	}
	g, err := s.Store.Groups.Create(body.DisplayName, members, s.Clock.Now())
	if errors.Is(err, store.ErrGroupExists) {
		writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS",
			"group "+body.DisplayName+" already exists")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, groupJSON(g))
}

// scimGroupsList pages with SCIM's 1-based startIndex/count.
//
// Honouring them is not a nicety. An unmodified databricks-sdk pages until it
// receives a short page; a handler that ignores startIndex and echoes the full
// list every time never terminates, and the client hangs forever rather than
// failing. The endpoint looks correct to any single-request test and is
// unusable by a real client — which is why this is witnessed by a paging test,
// not by a "list returns the group" assertion.
func (s *Server) scimGroupsList(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	all := s.Store.Groups.List()

	start := 1
	if v := r.URL.Query().Get("startIndex"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			start = n
		}
	}
	count := len(all)
	if v := r.URL.Query().Get("count"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			count = n
		}
	}

	page := []*store.Group{}
	if start <= len(all) {
		end := start - 1 + count
		if end > len(all) {
			end = len(all)
		}
		page = all[start-1 : end]
	}

	resources := make([]map[string]any, 0, len(page))
	for _, g := range page {
		resources = append(resources, groupJSON(g))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schemas":      []string{"urn:ietf:params:scim:api:messages:2.0:ListResponse"},
		"totalResults": len(all),
		"startIndex":   start,
		"itemsPerPage": len(resources),
		"Resources":    resources,
	})
}

func (s *Server) scimGroupsGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	g, err := s.Store.Groups.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "group not found")
		return
	}
	writeJSON(w, http.StatusOK, groupJSON(g))
}

func (s *Server) scimGroupsDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	if err := s.Store.Groups.Delete(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "group not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
