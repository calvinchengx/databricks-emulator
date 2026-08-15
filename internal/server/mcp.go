package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) mcpSQL(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Allow", "POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	case http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var req struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusOK, mcpRPC(nil, nil, &mcpErr{-32700, "parse error"}))
		return
	}
	if len(req.ID) == 0 || string(req.ID) == "null" {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	out := map[string]any{"jsonrpc": req.JSONRPC, "id": json.RawMessage(req.ID)}
	switch req.Method {
	case "initialize":
		out["result"] = map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "databricks-sql", "version": "emulator"},
			"instructions":    "Databricks SQL MCP on the emulator's Spark SQL warehouse surface, not Photon. Tools wrap POST /api/2.0/sql/statements.",
		}
		w.Header().Set("Mcp-Session-Id", fmt.Sprintf("mcp-%s", p.UserName))
	case "ping":
		out["result"] = map[string]any{}
	case "tools/list":
		out["result"] = map[string]any{"tools": mcpSQLTools}
	case "tools/call":
		out["result"] = s.mcpSQLCall(req.Params)
	default:
		out["error"] = mcpErr{-32601, "method not found: " + req.Method}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) mcpRefused(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
		"only /api/2.0/mcp/sql is implemented (Spark SQL on the warehouse surface). Genie, AI Search, and UC function MCP servers are refused")
}

func (s *Server) mcpSQLCall(params json.RawMessage) map[string]any {
	var call struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
		Meta      map[string]any `json:"_meta"`
	}
	if err := json.Unmarshal(params, &call); err != nil {
		return mcpToolErr("invalid tools/call params")
	}
	if call.Arguments == nil {
		call.Arguments = map[string]any{}
	}
	switch call.Name {
	case "execute_sql":
		return s.mcpExecuteSQL(str(call.Arguments["query"]), str(call.Meta["warehouse_id"]))
	case "poll_response":
		return s.mcpPollSQL(str(call.Arguments["statement_id"]))
	default:
		return mcpToolErr("unknown tool: " + call.Name)
	}
}

func (s *Server) mcpExecuteSQL(query, warehouseID string) map[string]any {
	if query == "" {
		return mcpToolErr("query is required")
	}
	var wh *store.Warehouse
	var ok bool
	if warehouseID != "" {
		wh, ok = s.Store.SQL.GetWarehouse(warehouseID)
		if !ok {
			return mcpToolErr("warehouse not found")
		}
	} else {
		wh, ok = s.Store.SQL.FirstRunning()
		if !ok {
			return mcpToolErr("no RUNNING SQL warehouse — create one or pass _meta.warehouse_id")
		}
	}
	st := s.Store.SQL.NewStatement(wh.ID, query)
	s.runSQLStatement(st, wh)
	return mcpToolOK(statementJSON(st))
}

func (s *Server) mcpPollSQL(id string) map[string]any {
	st, ok := s.Store.SQL.GetStatement(id)
	if !ok {
		return mcpToolErr("statement not found")
	}
	return mcpToolOK(statementJSON(st))
}

var mcpSQLTools = []map[string]any{
	{
		"name":        "execute_sql",
		"description": "Run Spark SQL on a SQL warehouse session handle (dialect spark-sql, not Photon)",
		"inputSchema": map[string]any{
			"type":       "object",
			"properties": map[string]any{"query": map[string]any{"type": "string"}},
			"required":   []string{"query"},
		},
	},
	{
		"name":        "poll_response",
		"description": "Poll a previously submitted SQL statement",
		"inputSchema": map[string]any{
			"type":       "object",
			"properties": map[string]any{"statement_id": map[string]any{"type": "string"}},
			"required":   []string{"statement_id"},
		},
	},
}

type mcpErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func mcpRPC(id json.RawMessage, result any, err *mcpErr) map[string]any {
	out := map[string]any{"jsonrpc": "2.0"}
	if len(id) > 0 {
		out["id"] = id
	}
	if err != nil {
		out["error"] = err
		return out
	}
	out["result"] = result
	return out
}

func mcpToolOK(v any) map[string]any {
	b, _ := json.Marshal(v)
	return map[string]any{"content": []map[string]any{{"type": "text", "text": string(b)}}}
}

func mcpToolErr(msg string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": msg}},
		"isError": true,
	}
}
