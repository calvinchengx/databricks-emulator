package server

import (
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestSQLQueriesCRUDHistoryAndAlertsRefused(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "q"}, &created); st != 200 {
		t.Fatalf("warehouse %d", st)
	}
	wh := str(created["id"])

	if st := h.json("GET", "/api/2.0/sql/queries", "dev", nil, nil); st != 401 {
		t.Fatalf("token=dev %d", st)
	}

	var q map[string]any
	if st := h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"query": map[string]any{
			"display_name": "one",
			"query_text":   "SELECT 1",
			"warehouse_id": wh,
			"description":  "saved",
			"tags":         []string{"e2e"},
		},
	}, &q); st != 200 {
		t.Fatalf("create %d %+v", st, q)
	}
	id := str(q["id"])
	if q["display_name"] != "one" || q["query_text"] != "SELECT 1" || q["warehouse_id"] != wh {
		t.Fatalf("create body %+v", q)
	}
	if q["lifecycle_state"] != "ACTIVE" || q["owner_user_name"] != "admin" {
		t.Fatalf("owner %+v", q)
	}

	var got map[string]any
	if st := h.json("GET", "/api/2.0/sql/queries/"+id, pat, nil, &got); st != 200 || got["id"] != id {
		t.Fatalf("get %d %+v", st, got)
	}
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/sql/queries", pat, nil, &listed); st != 200 {
		t.Fatalf("list %d", st)
	}
	results, _ := listed["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("list %+v", listed)
	}

	var dup map[string]any
	if st := h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"query": map[string]any{"display_name": "one", "query_text": "SELECT 2"},
	}, &dup); st != 409 {
		t.Fatalf("dup %d %+v", st, dup)
	}
	var resolved map[string]any
	if st := h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"auto_resolve_display_name": true,
		"query":                     map[string]any{"display_name": "one", "query_text": "SELECT 2"},
	}, &resolved); st != 200 || resolved["display_name"] != "one (1)" {
		t.Fatalf("auto %d %+v", st, resolved)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "sql" || req.Code != "SELECT 1" {
			t.Fatalf("engine %+v", req)
		}
		return spark.Result{OK: true, Stdout: `[{"1":1}]`}, nil
	}
	var execd map[string]any
	if st := h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": wh, "statement": "SELECT 1",
	}, &execd); st != 200 {
		t.Fatalf("exec %d %+v", st, execd)
	}
	if execd["status"].(map[string]any)["state"] != "SUCCEEDED" {
		t.Fatalf("exec %+v", execd)
	}

	var hist map[string]any
	if st := h.json("GET", "/api/2.0/sql/history/queries?filter_by.warehouse_ids="+wh, pat, nil, &hist); st != 200 {
		t.Fatalf("history %d %+v", st, hist)
	}
	res, _ := hist["res"].([]any)
	if len(res) != 1 {
		t.Fatalf("history %+v", hist)
	}
	row, _ := res[0].(map[string]any)
	if row["query_text"] != "SELECT 1" || row["status"] != "FINISHED" || row["warehouse_id"] != wh {
		t.Fatalf("row %+v", row)
	}
	if row["query_id"] != execd["statement_id"] || row["user_name"] != "admin" {
		t.Fatalf("link %+v", row)
	}

	var patched map[string]any
	if st := h.json("PATCH", "/api/2.0/sql/queries/"+id, pat, map[string]any{
		"update_mask": "query_text,description",
		"query":       map[string]any{"query_text": "SELECT 3", "description": "edited"},
	}, &patched); st != 200 || patched["query_text"] != "SELECT 3" || patched["description"] != "edited" {
		t.Fatalf("patch %d %+v", st, patched)
	}
	if patched["display_name"] != "one" {
		t.Fatalf("mask leaked %+v", patched)
	}

	if st := h.json("DELETE", "/api/2.0/sql/queries/"+id, pat, nil, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
	var after map[string]any
	h.json("GET", "/api/2.0/sql/queries", pat, nil, &after)
	left, _ := after["results"].([]any)
	if len(left) != 1 { // the auto-resolved "one (1)" remains
		t.Fatalf("after trash %+v", after)
	}
	var trashed map[string]any
	if st := h.json("GET", "/api/2.0/sql/queries/"+id, pat, nil, &trashed); st != 200 || trashed["lifecycle_state"] != "TRASHED" {
		t.Fatalf("trashed get %d %+v", st, trashed)
	}
	if st := h.json("DELETE", "/api/2.0/sql/queries/"+id, pat, nil, nil); st != 404 {
		t.Fatalf("double trash %d", st)
	}

	if st := h.json("POST", "/api/2.0/sql/alerts", pat, map[string]any{}, nil); st != 501 {
		t.Fatalf("alerts %d", st)
	}
	if st := h.json("GET", "/api/2.0/sql/queries/"+id+"/visualizations", pat, nil, nil); st != 501 {
		t.Fatalf("viz %d", st)
	}
}

func TestSQLQueriesMissingEngineUnknownAndBadBodies(t *testing.T) {
	h := newHarness(t)
	h.srv.Spark = nil
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "q"}, &created)
	wh := str(created["id"])

	if st := h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{}, nil); st != 400 {
		t.Fatalf("empty %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/queries", pat, `{`, nil); st != 400 {
		t.Fatalf("bad body %d", st)
	}
	if st := h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"query": map[string]any{"display_name": "x", "query_text": "SELECT 1", "warehouse_id": "nope"},
	}, nil); st != 404 {
		t.Fatalf("unknown warehouse %d", st)
	}
	if st := h.json("GET", "/api/2.0/sql/queries/missing", pat, nil, nil); st != 404 {
		t.Fatalf("get missing %d", st)
	}
	if st := h.json("PATCH", "/api/2.0/sql/queries/missing", pat, map[string]any{
		"update_mask": "query_text", "query": map[string]any{"query_text": "SELECT 1"},
	}, nil); st != 404 {
		t.Fatalf("patch missing %d", st)
	}
	var q map[string]any
	h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"query": map[string]any{"display_name": "x", "query_text": "SELECT 1", "warehouse_id": wh},
	}, &q)
	if st := h.json("PATCH", "/api/2.0/sql/queries/"+str(q["id"]), pat, map[string]any{
		"query": map[string]any{"query_text": "SELECT 2"},
	}, nil); st != 400 {
		t.Fatalf("no mask %d", st)
	}
	if st := h.json("PATCH", "/api/2.0/sql/queries/"+str(q["id"]), pat, `{`, nil); st != 400 {
		t.Fatalf("bad patch %d", st)
	}
	if st := h.json("PATCH", "/api/2.0/sql/queries/"+str(q["id"]), pat, map[string]any{
		"update_mask": "warehouse_id", "query": map[string]any{"warehouse_id": "nope"},
	}, nil); st != 404 {
		t.Fatalf("patch unknown warehouse %d", st)
	}

	var execd map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{
		"warehouse_id": wh, "statement": "SELECT 1",
	}, &execd)
	if execd["status"].(map[string]any)["state"] != "FAILED" {
		t.Fatalf("no engine %+v", execd)
	}
	errObj, _ := execd["status"].(map[string]any)["error"].(map[string]any)
	if !strings.Contains(str(errObj["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("must name engine %+v", execd)
	}
	var hist map[string]any
	h.json("GET", "/api/2.0/sql/history/queries?filter_by.statuses=FAILED", pat, nil, &hist)
	rows, _ := hist["res"].([]any)
	if len(rows) != 1 {
		t.Fatalf("failed history %+v", hist)
	}
	row, _ := rows[0].(map[string]any)
	if !strings.Contains(str(row["error_message"]), "DATABRICKS_SPARK_CONNECT_URL") || row["status"] != "FAILED" {
		t.Fatalf("history error %+v", row)
	}

	if st := h.json("GET", "/api/2.0/preview/sql/alerts/x", pat, nil, nil); st != 501 {
		t.Fatalf("preview alerts %d", st)
	}
	if st := h.json("GET", "/api/2.0/alerts", pat, nil, nil); st != 501 {
		t.Fatalf("alerts v2 %d", st)
	}

	if st := h.json("DELETE", "/api/2.0/sql/queries/"+str(q["id"]), pat, nil, nil); st != 200 {
		t.Fatalf("trash for patch %d", st)
	}
	if st := h.json("PATCH", "/api/2.0/sql/queries/"+str(q["id"]), pat, map[string]any{
		"update_mask": "query_text", "query": map[string]any{"query_text": "SELECT 9"},
	}, nil); st != 404 {
		t.Fatalf("patch trashed %d", st)
	}
}

func TestSQLQueryHistoryFiltersAndPagination(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "q"}, &created)
	wh := str(created["id"])
	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: true, Stdout: "ok"}, nil
	}
	var first, second map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{"warehouse_id": wh, "statement": "SELECT 1"}, &first)
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{"warehouse_id": wh, "statement": "INSERT INTO t VALUES (1)"}, &second)

	var page map[string]any
	if st := h.json("GET", "/api/2.0/sql/history/queries?max_results=1", pat, nil, &page); st != 200 {
		t.Fatalf("page %d", st)
	}
	if page["has_next_page"] != true || str(page["next_page_token"]) == "" {
		t.Fatalf("page %+v", page)
	}
	rows, _ := page["res"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["query_text"] != "INSERT INTO t VALUES (1)" {
		t.Fatalf("newest %+v", page)
	}
	var next map[string]any
	h.json("GET", "/api/2.0/sql/history/queries?max_results=1&page_token="+str(page["next_page_token"]), pat, nil, &next)
	if next["has_next_page"] != false || next["res"].([]any)[0].(map[string]any)["query_id"] != first["statement_id"] {
		t.Fatalf("next %+v", next)
	}

	var byStmt map[string]any
	h.json("GET", "/api/2.0/sql/history/queries?filter_by.statement_ids="+str(second["statement_id"]), pat, nil, &byStmt)
	if len(byStmt["res"].([]any)) != 1 || byStmt["res"].([]any)[0].(map[string]any)["statement_type"] != "INSERT" {
		t.Fatalf("by stmt %+v", byStmt)
	}

	var q1, q2 map[string]any
	h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"query": map[string]any{"display_name": "a", "query_text": "SELECT 1"},
	}, &q1)
	h.json("POST", "/api/2.0/sql/queries", pat, map[string]any{
		"query": map[string]any{"display_name": "b", "query_text": "SELECT 2"},
	}, &q2)
	var qpage map[string]any
	h.json("GET", "/api/2.0/sql/queries?page_size=1", pat, nil, &qpage)
	if str(qpage["next_page_token"]) == "" || len(qpage["results"].([]any)) != 1 {
		t.Fatalf("q page %+v", qpage)
	}
	var qnext map[string]any
	h.json("GET", "/api/2.0/sql/queries?page_size=1&page_token="+str(qpage["next_page_token"]), pat, nil, &qnext)
	if qnext["next_page_token"] != nil || qnext["results"].([]any)[0].(map[string]any)["id"] != q2["id"] {
		t.Fatalf("q next %+v first=%v", qnext, q1)
	}

	if st := h.json("PATCH", "/api/2.0/sql/queries/"+str(q1["id"]), pat, map[string]any{
		"update_mask": "*",
		"query": map[string]any{
			"display_name": "renamed", "query_text": "SELECT 9", "warehouse_id": wh,
			"catalog": "main", "schema": "default", "run_as_mode": "VIEWER",
		},
	}, &q1); st != 200 || q1["display_name"] != "renamed" || q1["run_as_mode"] != "VIEWER" || q1["catalog"] != "main" {
		t.Fatalf("star %d %+v", st, q1)
	}
	if st := h.json("PATCH", "/api/2.0/sql/queries/"+str(q1["id"]), pat, map[string]any{
		"update_mask": "display_name", "query": map[string]any{"display_name": "b"},
	}, nil); st != 409 {
		t.Fatalf("rename clash %d", st)
	}

	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: true, Stdout: "ok"}, nil
	}
	var vac map[string]any
	h.json("POST", "/api/2.0/sql/statements", pat, map[string]any{"warehouse_id": wh, "statement": "VACUUM events"}, &vac)
	var other map[string]any
	h.json("GET", "/api/2.0/sql/history/queries?filter_by.statement_ids="+str(vac["statement_id"]), pat, nil, &other)
	if other["res"].([]any)[0].(map[string]any)["statement_type"] != "OTHER" {
		t.Fatalf("vacuum type %+v", other)
	}
}
