package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) policiesCreate(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		Name         string `json:"name"`
		Definition   string `json:"definition"`
		Description  string `json:"description"`
		PolicyFamily string `json:"policy_family_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	def := body.Definition
	if def == "" && body.PolicyFamily != "" {
		if body.PolicyFamily != store.EmulatorSessionFamilyID {
			writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "policy family not found")
			return
		}
		def = store.EmulatorSessionFamilyDefinition
	}
	pol, err := s.Store.Policies.CreatePolicy(body.Name, def, body.Description, body.PolicyFamily, p.UserName, s.Clock.Now())
	if err != nil {
		status, code := policyErr(err)
		writeError(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy_id": pol.ID})
}

func (s *Server) policiesGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := query(r, "policy_id")
	pol, ok := s.Store.Policies.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "policy not found")
		return
	}
	writeJSON(w, http.StatusOK, policyJSON(pol))
}

func (s *Server) policiesList(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	list := []map[string]any{}
	for _, pol := range s.Store.Policies.List() {
		list = append(list, policyJSON(pol))
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": list})
}

func (s *Server) policiesEdit(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		PolicyID    string `json:"policy_id"`
		Name        string `json:"name"`
		Definition  string `json:"definition"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	pol, err := s.Store.Policies.Edit(body.PolicyID, body.Name, body.Definition, body.Description)
	if err != nil {
		status, code := policyErr(err)
		writeError(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, policyJSON(pol))
}

func (s *Server) policiesDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		PolicyID string `json:"policy_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if !s.Store.Policies.Delete(body.PolicyID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "policy not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) policiesGetCompliance(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := query(r, "cluster_id")
	cl, ok := s.Store.Clusters.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "cluster not found")
		return
	}
	if cl.PolicyID == "" {
		writeJSON(w, http.StatusOK, map[string]any{"is_compliant": true, "violations": map[string]string{}})
		return
	}
	pol, ok := s.Store.Policies.Get(cl.PolicyID)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"is_compliant": false,
			"violations":   map[string]string{"policy_id": "policy no longer exists"},
		})
		return
	}
	violations, err := store.EvaluatePolicy(pol.Definition, clusterAttrs(cl, false, false))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"is_compliant": len(violations) == 0,
		"violations":   violations,
	})
}

func (s *Server) policyFamiliesList(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeJSON(w, http.StatusOK, map[string]any{
		"policy_families": []map[string]any{{
			"policy_family_id": store.EmulatorSessionFamilyID,
			"name":             "Emulator session",
			"description":      "Session handle onto the attached Spark engine, not a VM",
			"definition":       store.EmulatorSessionFamilyDefinition,
		}},
	})
}

func policyJSON(p *store.Policy) map[string]any {
	out := map[string]any{
		"policy_id":            p.ID,
		"name":                 p.Name,
		"definition":           p.Definition,
		"creator_user_name":    p.Creator,
		"created_at_timestamp": p.CreatedAt,
	}
	if p.Description != "" {
		out["description"] = p.Description
	}
	if p.FamilyID != "" {
		out["policy_family_id"] = p.FamilyID
	}
	return out
}

func policyErr(err error) (int, string) {
	msg := err.Error()
	if msg == "policy not found" {
		return http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST"
	}
	if strings.Contains(msg, "not implemented") || strings.Contains(msg, "not enforced") || strings.Contains(msg, "not stored-and-ignored") {
		return http.StatusNotImplemented, "NOT_IMPLEMENTED"
	}
	return http.StatusBadRequest, "BAD_REQUEST"
}

func clusterAttrs(cl *store.Cluster, autoscale, libraries bool) store.ClusterAttrs {
	return store.ClusterAttrs{
		SparkVersion: cl.SparkVersion,
		NodeTypeID:   cl.NodeTypeID,
		NumWorkers:   cl.NumWorkers,
		Autoscale:    autoscale,
		Libraries:    libraries,
	}
}

func (s *Server) enforcePolicy(policyID string, attrs *store.ClusterAttrs) error {
	if policyID == "" {
		return nil
	}
	pol, ok := s.Store.Policies.Get(policyID)
	if !ok {
		return fmt.Errorf("policy not found")
	}
	*attrs = store.ApplyFixedDefaults(pol.Definition, *attrs)
	violations, err := store.EvaluatePolicy(pol.Definition, *attrs)
	if err != nil {
		return err
	}
	if len(violations) == 0 {
		return nil
	}
	var parts []string
	for k, msg := range violations {
		parts = append(parts, k+": "+msg)
	}
	return fmt.Errorf("cluster does not comply with policy %s (%s)", policyID, strings.Join(parts, "; "))
}
