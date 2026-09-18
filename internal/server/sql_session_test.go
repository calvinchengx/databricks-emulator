package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

// catalogPerSession is the agent's behaviour from fabric-emulator v0.33.0 on
// Sail: every agent session gets its own Spark Connect session, and that
// session starts with an EMPTY catalog. A fake rather than the real agent
// because the defect is not in the engine -- it is in which session this
// server asks -- and because a unit test that models the catalog fails for the
// reason the e2e suites failed, without Docker.
type catalogPerSession struct {
	tables map[string]map[string]bool // session -> table names it can see
}

func (c *catalogPerSession) run(req spark.Request) (spark.Result, error) {
	if c.tables == nil {
		c.tables = map[string]map[string]bool{}
	}
	if c.tables[req.Session] == nil {
		c.tables[req.Session] = map[string]bool{}
	}
	seen := c.tables[req.Session]
	code := strings.TrimSpace(req.Code)
	switch {
	case strings.HasPrefix(strings.ToUpper(code), "CREATE TABLE"):
		seen[tableIn(code, "CREATE TABLE")] = true
		return spark.Result{OK: true, Stdout: "[]"}, nil
	case strings.HasPrefix(strings.ToUpper(code), "INSERT INTO"):
		name := tableIn(code, "INSERT INTO")
		if !seen[name] {
			// The message Sail actually returned, abbreviated.
			return spark.Result{
				EName:  "IllegalArgumentException",
				EValue: fmt.Sprintf("table does not exist: %s", name),
			}, nil
		}
		return spark.Result{OK: true, Stdout: "[]"}, nil
	default:
		return spark.Result{OK: true, Stdout: "[]"}, nil
	}
}

func tableIn(code, prefix string) string {
	rest := strings.TrimSpace(code[len(prefix):])
	name, _, _ := strings.Cut(rest, " ")
	return strings.Trim(name, "`")
}

// A table created by one warehouse statement is there for the next one.
func TestWarehouseStatementsShareOneCatalog(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{
		"name": "starter", "cluster_size": "2X-Small",
	}, &created); st != 200 {
		t.Fatalf("create warehouse %d", st)
	}
	id := str(created["id"])

	engine := &catalogPerSession{}
	h.exec.Hook = engine.run

	exec := func(sql string) map[string]any {
		t.Helper()
		var out map[string]any
		if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
			"warehouse_id": id, "statement": sql,
		}, &out); st != 200 {
			t.Fatalf("execute %q: status %d", sql, st)
		}
		return out
	}

	if state := statementState(exec("CREATE TABLE events (id INT)")); state != "SUCCEEDED" {
		t.Fatalf("create: %s", state)
	}
	insert := exec("INSERT INTO events VALUES (1)")
	if state := statementState(insert); state != "SUCCEEDED" {
		t.Fatalf("a statement could not see the table the one before it created: %s %+v",
			state, insert["status"])
	}

	if len(h.exec.Calls) < 2 {
		t.Fatalf("both statements must reach the engine, got %d", len(h.exec.Calls))
	}
	for _, call := range h.exec.Calls {
		if call.Session != spark.WarehouseSession {
			t.Fatalf("statement ran in session %q, want %q", call.Session, spark.WarehouseSession)
		}
	}
}

// Two warehouses share the catalog too: on Databricks the metastore is not
// per-warehouse, and a second warehouse reading the first one's table is the
// shape that says so.
func TestWarehousesShareTheCatalogWithEachOther(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	warehouse := func(name string) string {
		t.Helper()
		var created map[string]any
		if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{
			"name": name, "cluster_size": "2X-Small",
		}, &created); st != 200 {
			t.Fatalf("create warehouse %s: %d", name, st)
		}
		return str(created["id"])
	}
	first, second := warehouse("one"), warehouse("two")
	if first == second {
		t.Fatal("two warehouses, one id")
	}

	engine := &catalogPerSession{}
	h.exec.Hook = engine.run

	run := func(warehouseID, sql string) map[string]any {
		t.Helper()
		var out map[string]any
		if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
			"warehouse_id": warehouseID, "statement": sql,
		}, &out); st != 200 {
			t.Fatalf("execute %q: status %d", sql, st)
		}
		return out
	}

	if state := statementState(run(first, "CREATE TABLE shared (id INT)")); state != "SUCCEEDED" {
		t.Fatalf("create on the first warehouse: %s", state)
	}
	if state := statementState(run(second, "INSERT INTO shared VALUES (1)")); state != "SUCCEEDED" {
		t.Fatalf("the second warehouse could not see the first one's table: %s", state)
	}
	if len(engine.tables) != 1 {
		t.Fatalf("warehouses opened %d catalogs, want 1: %v", len(engine.tables), engine.tables)
	}
}

// The engine's own refusal still reaches the caller: sharing a session must
// not turn a failed statement into a pass.
func TestWarehouseStatementStillFailsWhenTheEngineRefuses(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{
		"name": "starter", "cluster_size": "2X-Small",
	}, &created); st != 200 {
		t.Fatalf("create warehouse %d", st)
	}
	engine := &catalogPerSession{}
	h.exec.Hook = engine.run

	var out map[string]any
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": str(created["id"]), "statement": "INSERT INTO missing VALUES (1)",
	}, &out); st != 200 {
		t.Fatalf("execute %d", st)
	}
	if state := statementState(out); state != "FAILED" {
		t.Fatalf("state %s, want FAILED: %+v", state, out)
	}
	status, _ := out["status"].(map[string]any)
	if e, _ := status["error"].(map[string]any); e == nil || !strings.Contains(str(e["message"]), "table does not exist") {
		t.Fatalf("the engine's reason did not survive: %+v", status)
	}
}

func statementState(out map[string]any) string {
	status, _ := out["status"].(map[string]any)
	return str(status["state"])
}

// dbt reaches the warehouse over HiveServer2, not the statements API, and that
// is the path the empty-catalog defect was first seen on ("Database not found:
// hive_metastore.default"). Thrift has a session concept of its own, so this
// pins that two thrift connections still land in ONE agent session.
func TestThriftStatementsShareTheWarehouseSession(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "thrift"}, &created); st != 200 {
		t.Fatalf("create warehouse %d", st)
	}
	engine := &catalogPerSession{}
	h.exec.Hook = engine.run

	ctx := context.Background()
	exec := func(sql string) *cliservice.TExecuteStatementResp {
		t.Helper()
		cli := thriftClient(t, h, "/sql/1.0/endpoints/"+str(created["id"]), pat)
		opened, err := cli.OpenSession(ctx, &cliservice.TOpenSessionReq{
			ClientProtocolI64: protoI64(cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7),
		})
		if err != nil {
			t.Fatal(err)
		}
		out, err := cli.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
			SessionHandle:    opened.SessionHandle,
			Statement:        sql,
			GetDirectResults: &cliservice.TSparkGetDirectResults{MaxRows: 1000},
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	if out := exec("CREATE TABLE events (id INT)"); out.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("create over thrift: %+v", out.Status)
	}
	// A SECOND connection, the way dbt opens one per operation.
	out := exec("INSERT INTO events VALUES (1)")
	if out.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("a second thrift connection could not see the table: %+v", out.Status)
	}
	if len(engine.tables) != 1 {
		t.Fatalf("thrift opened %d catalogs, want 1: %v", len(engine.tables), engine.tables)
	}
	for _, call := range h.exec.Calls {
		if call.Session != spark.WarehouseSession {
			t.Fatalf("thrift statement ran in session %q", call.Session)
		}
	}
}

// A statement the rewriter refuses never reaches the engine, and the refusal
// is the caller's error. Shares the session constant's path up to the point
// the plan is checked, so it belongs with these.
func TestRefusedRewriteNeverReachesTheEngine(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "starter"}, &created); st != 200 {
		t.Fatalf("create warehouse %d", st)
	}
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		t.Fatalf("an unsafe identifier reached the engine: %q", req.Code)
		return spark.Result{}, nil
	}
	var out map[string]any
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": str(created["id"]),
		// `..` would walk out of the Delta root once interpolated into LOCATION.
		"statement": "CREATE TABLE `cat`.`..`.`t` USING delta AS SELECT 1 AS id",
	}, &out); st != 200 {
		t.Fatalf("execute %d", st)
	}
	if state := statementState(out); state != "FAILED" {
		t.Fatalf("state %s, want FAILED: %+v", state, out)
	}
	status, _ := out["status"].(map[string]any)
	e, _ := status["error"].(map[string]any)
	if e == nil || !strings.Contains(str(e["message"]), "managed location") {
		t.Fatalf("refusal did not name the reason: %+v", status)
	}
	if len(h.exec.Calls) != 0 {
		t.Fatalf("engine calls on a refused statement: %d", len(h.exec.Calls))
	}
}
