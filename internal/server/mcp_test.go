package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestMCPSQLExecuteAndPoll(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "starter"}, &created)
	id := str(created["id"])
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "sql" || !strings.Contains(req.Code, "SELECT 1") {
			t.Fatalf("engine %+v", req)
		}
		return spark.Result{OK: true, Stdout: `[{"1":1}]`}, nil
	}

	if st := h.json("POST", "/api/2.0/mcp/sql", "", map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
	}, nil); st != 401 {
		t.Fatalf("no pat %d", st)
	}

	var init map[string]any
	resp := h.do("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{"protocolVersion": "2025-03-26"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("init %d sid=%q", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"))
	}
	_ = json.NewDecoder(resp.Body).Decode(&init)
	if init["result"].(map[string]any)["serverInfo"].(map[string]any)["name"] != "databricks-sql" {
		t.Fatalf("init %+v", init)
	}

	var listed map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/list",
	}, &listed)
	tools := listed["result"].(map[string]any)["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("tools %+v", listed)
	}

	var ping map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "ping",
	}, &ping)

	var execd map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 4, "method": "tools/call",
		"params": map[string]any{
			"name":      "execute_sql",
			"arguments": map[string]any{"query": "SELECT 1"},
			"_meta":     map[string]any{"warehouse_id": id},
		},
	}, &execd)
	text := execd["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"]
	if !strings.Contains(str(text), "spark-sql") || !strings.Contains(str(text), "SUCCEEDED") {
		t.Fatalf("execute %+v", execd)
	}
	if len(h.exec.Calls) == 0 {
		t.Fatal("SUCCESS without reaching the engine")
	}
	var stmt struct {
		StatementID string `json:"statement_id"`
	}
	_ = json.Unmarshal([]byte(str(text)), &stmt)

	var polled map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 5, "method": "tools/call",
		"params": map[string]any{
			"name":      "poll_response",
			"arguments": map[string]any{"statement_id": stmt.StatementID},
		},
	}, &polled)
	if !strings.Contains(str(polled["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"]), stmt.StatementID) {
		t.Fatalf("poll %+v", polled)
	}
}

func TestMCPSQLPicksRunningWarehouseAndRefusesOthers(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "auto"}, nil)

	var execd map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{
			"name":      "execute_sql",
			"arguments": map[string]any{"query": "SELECT 2"},
		},
	}, &execd)
	if !strings.Contains(str(execd["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"]), "SUCCEEDED") {
		t.Fatalf("auto warehouse %+v", execd)
	}

	var missing map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/call",
		"params": map[string]any{
			"name":      "execute_sql",
			"arguments": map[string]any{"query": "SELECT 1"},
			"_meta":     map[string]any{"warehouse_id": "nope"},
		},
	}, &missing)
	if missing["result"].(map[string]any)["isError"] != true {
		t.Fatalf("missing warehouse %+v", missing)
	}

	var unknown map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "tools/call",
		"params": map[string]any{"name": "drop_all"},
	}, &unknown)
	if unknown["result"].(map[string]any)["isError"] != true {
		t.Fatalf("unknown tool %+v", unknown)
	}

	var noQuery map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 4, "method": "tools/call",
		"params": map[string]any{"name": "execute_sql", "arguments": map[string]any{}},
	}, &noQuery)
	if noQuery["result"].(map[string]any)["isError"] != true {
		t.Fatalf("empty query %+v", noQuery)
	}

	var noStmt map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 5, "method": "tools/call",
		"params": map[string]any{"name": "poll_response", "arguments": map[string]any{"statement_id": "nope"}},
	}, &noStmt)
	if noStmt["result"].(map[string]any)["isError"] != true {
		t.Fatalf("missing stmt %+v", noStmt)
	}

	var badMethod map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 6, "method": "tools/explode",
	}, &badMethod)
	if badMethod["error"] == nil {
		t.Fatalf("unknown method %+v", badMethod)
	}

	if st := h.json("POST", "/api/2.0/mcp/sql", pat, `{`, nil); st != 200 {
		t.Fatalf("parse %d", st)
	}
	if st := h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{"jsonrpc": "2.0", "method": "ping"}, nil); st != 202 {
		t.Fatalf("notify %d", st)
	}
	if st := h.json("GET", "/api/2.0/mcp/sql", pat, nil, nil); st != 405 {
		t.Fatalf("get %d", st)
	}
	if st := h.json("DELETE", "/api/2.0/mcp/sql", pat, nil, nil); st != 204 {
		t.Fatalf("delete %d", st)
	}
	if st := h.json("GET", "/api/2.0/mcp/genie", pat, nil, nil); st != 501 {
		t.Fatalf("genie %d", st)
	}
	if st := h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": `{`,
	}, nil); st != 200 {
		t.Fatalf("bad call params %d", st)
	}
}

func TestMCPSQLNoWarehouse(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var execd map[string]any
	h.json("POST", "/api/2.0/mcp/sql", pat, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{
			"name":      "execute_sql",
			"arguments": map[string]any{"query": "SELECT 1"},
		},
	}, &execd)
	if execd["result"].(map[string]any)["isError"] != true {
		t.Fatalf("no warehouse %+v", execd)
	}
}
