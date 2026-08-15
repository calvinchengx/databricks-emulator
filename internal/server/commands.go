package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) contextsCreate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		ClusterID string `json:"clusterId"`
		Language  string `json:"language"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	kind, err := commandKind(body.Language)
	if err != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
		return
	}
	cl, ok := s.Store.Clusters.Get(body.ClusterID)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "cluster not found")
		return
	}
	if cl.State != "RUNNING" {
		writeError(w, http.StatusBadRequest, "INVALID_STATE", "cluster is not RUNNING")
		return
	}
	if s.Spark == nil {
		writeError(w, http.StatusBadRequest, "INVALID_STATE", errNoSparkEngine.Error())
		return
	}
	ctx := s.Store.Commands.CreateContext(body.ClusterID, strings.ToLower(body.Language))
	probe := "print(1)\n"
	if kind == "sql" {
		probe = "SELECT 1"
	}
	if err := s.pingContext(ctx.ID, kind, probe); err != nil {
		s.Store.Commands.DestroyContext(ctx.ID)
		writeError(w, http.StatusBadRequest, "INVALID_STATE", err.Error())
		return
	}
	s.Store.Commands.SetContextStatus(ctx.ID, "Running")
	writeJSON(w, http.StatusOK, map[string]any{"id": ctx.ID})
}

func (s *Server) contextsStatus(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := query(r, "contextId")
	ctx, ok := s.Store.Commands.GetContext(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "context not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": ctx.ID, "status": ctx.Status})
}

func (s *Server) contextsDestroy(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		ContextID string `json:"contextId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if !s.Store.Commands.DestroyContext(body.ContextID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "context not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) commandsExecute(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		ClusterID string `json:"clusterId"`
		ContextID string `json:"contextId"`
		Language  string `json:"language"`
		Command   string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if strings.TrimSpace(body.Command) == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "command is required")
		return
	}
	kind, err := commandKind(body.Language)
	if err != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
		return
	}
	ctx, ok := s.Store.Commands.GetContext(body.ContextID)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "context not found")
		return
	}
	if ctx.Status != "Running" {
		writeError(w, http.StatusBadRequest, "INVALID_STATE", "context is not Running")
		return
	}
	if body.ClusterID != "" && body.ClusterID != ctx.ClusterID {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "clusterId does not match the context")
		return
	}
	cmd := s.Store.Commands.CreateCommand(ctx.ClusterID, ctx.ID, ctx.Language, body.Command)
	s.runCommand(cmd, kind)
	writeJSON(w, http.StatusOK, map[string]any{"id": cmd.ID})
}

func (s *Server) commandsStatus(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := query(r, "commandId")
	cmd, ok := s.Store.Commands.GetCommand(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "command not found")
		return
	}
	writeJSON(w, http.StatusOK, commandJSON(cmd))
}

func (s *Server) commandsCancel(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		CommandID string `json:"commandId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if !s.Store.Commands.CancelCommand(body.CommandID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "command not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) runCommand(cmd *store.ExecCommand, kind string) {
	if s.Spark == nil {
		s.Store.Commands.FinishCommand(cmd.ID, "Error", "error", "", errNoSparkEngine.Error(), "")
		return
	}
	res, err := s.Spark.Run(spark.Request{
		Session: "context-" + cmd.ContextID,
		Kind:    kind,
		Code:    cmd.Code,
	})
	if err != nil {
		s.Store.Commands.FinishCommand(cmd.ID, "Error", "error", "", err.Error(), "")
		return
	}
	if !res.OK {
		msg := res.EValue
		if msg == "" {
			msg = res.EName
		}
		if msg == "" {
			msg = "command failed"
		}
		s.Store.Commands.FinishCommand(cmd.ID, "Error", "error", "", msg, res.EName)
		return
	}
	s.Store.Commands.FinishCommand(cmd.ID, "Finished", "text", res.Stdout, "", "")
}

func (s *Server) pingContext(contextID, kind, code string) error {
	res, err := s.Spark.Run(spark.Request{
		Session: "context-" + contextID,
		Kind:    kind,
		Code:    code,
	})
	if err != nil {
		return err
	}
	if !res.OK {
		msg := res.EValue
		if msg == "" {
			msg = res.EName
		}
		if msg == "" {
			msg = "context failed to start"
		}
		return errString(msg)
	}
	return nil
}

func commandKind(language string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "", "python":
		return "python", nil
	case "sql":
		return "sql", nil
	case "scala", "r":
		return "", errString("Command Execution language " + language + " is not implemented; python and sql run on the attached Sail agent")
	default:
		return "", errString("Command Execution language " + language + " is not implemented; python and sql run on the attached Sail agent")
	}
}

func commandJSON(cmd *store.ExecCommand) map[string]any {
	out := map[string]any{
		"id":     cmd.ID,
		"status": cmd.Status,
	}
	results := map[string]any{}
	if cmd.ResultType != "" {
		results["resultType"] = cmd.ResultType
	}
	if cmd.Data != "" {
		results["data"] = cmd.Data
	}
	if cmd.Summary != "" {
		results["summary"] = cmd.Summary
	}
	if cmd.Cause != "" {
		results["cause"] = cmd.Cause
	}
	if len(results) > 0 {
		out["results"] = results
	}
	return out
}
