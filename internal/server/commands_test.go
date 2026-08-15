package server

import (
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestCommandExecutionRunsOnSailAndRefusesScala(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if strings.Contains(req.Code, "CMD-REACHED") {
			if req.Kind != "python" || !strings.HasPrefix(req.Session, "context-") {
				t.Fatalf("execute %+v", req)
			}
			return spark.Result{OK: true, Stdout: "CMD-REACHED\n"}, nil
		}
		return spark.Result{OK: true, Stdout: "1"}, nil
	}
	var created map[string]any
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{
		"cluster_name": "cmd",
	}, &created); st != 200 {
		t.Fatalf("cluster %d %+v", st, created)
	}
	clusterID := str(created["cluster_id"])

	if st := h.json("POST", "/api/1.2/contexts/create", "", nil, nil); st != 401 {
		t.Fatalf("unauth %d", st)
	}
	var ctx map[string]any
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "python",
	}, &ctx); st != 200 || str(ctx["id"]) == "" {
		t.Fatalf("create ctx %d %+v", st, ctx)
	}
	ctxID := str(ctx["id"])
	var status map[string]any
	if st := h.json("GET", "/api/1.2/contexts/status?clusterId="+clusterID+"&contextId="+ctxID, pat, nil, &status); st != 200 || status["status"] != "Running" {
		t.Fatalf("ctx status %d %+v", st, status)
	}

	var body map[string]any
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "scala",
	}, &body); st != 501 || !strings.Contains(str(body["message"]), "scala") {
		t.Fatalf("scala %d %+v", st, body)
	}

	var execd map[string]any
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"clusterId": clusterID, "contextId": ctxID, "language": "python", "command": "print('CMD-REACHED')",
	}, &execd); st != 200 {
		t.Fatalf("execute %d %+v", st, execd)
	}
	cmdID := str(execd["id"])
	var cmd map[string]any
	if st := h.json("GET", "/api/1.2/commands/status?clusterId="+clusterID+"&contextId="+ctxID+"&commandId="+cmdID, pat, nil, &cmd); st != 200 {
		t.Fatalf("status %d", st)
	}
	if cmd["status"] != "Finished" {
		t.Fatalf("status %+v", cmd)
	}
	results, _ := cmd["results"].(map[string]any)
	if results["resultType"] != "text" || !strings.Contains(str(results["data"]), "CMD-REACHED") {
		t.Fatalf("results %+v", cmd)
	}

	if st := h.json("POST", "/api/1.2/commands/cancel", pat, map[string]any{"commandId": cmdID}, nil); st != 200 {
		t.Fatalf("cancel %d", st)
	}
	if st := h.json("POST", "/api/1.2/contexts/destroy", pat, map[string]any{
		"clusterId": clusterID, "contextId": ctxID,
	}, nil); st != 200 {
		t.Fatalf("destroy %d", st)
	}
	if st := h.json("GET", "/api/1.2/contexts/status?contextId="+ctxID, pat, nil, nil); st != 404 {
		t.Fatalf("destroyed %d", st)
	}
}

func TestCommandExecutionNoEngineFailsNamingIt(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{
		"cluster_name": "cmd",
	}, &created); st != 200 {
		t.Fatalf("cluster %d %+v", st, created)
	}
	h.srv.Spark = nil
	var body map[string]any
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": str(created["cluster_id"]), "language": "python",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("no engine %d %+v", st, body)
	}
}

func TestCommandExecutionMissingClusterIs404(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": "nope", "language": "python",
	}, nil); st != 404 {
		t.Fatalf("missing cluster %d", st)
	}
}

func TestCommandExecutionSQLUsesKindSQL(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Code == "SELECT 2" {
			if req.Kind != "sql" {
				t.Fatalf("sql kind %+v", req)
			}
			return spark.Result{OK: true, Stdout: "2"}, nil
		}
		return spark.Result{OK: true, Stdout: "1"}, nil
	}
	var created map[string]any
	h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{"cluster_name": "sql"}, &created)
	var ctx map[string]any
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": str(created["cluster_id"]), "language": "sql",
	}, &ctx); st != 200 {
		t.Fatalf("sql ctx %d %+v", st, ctx)
	}
	var execd map[string]any
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"clusterId": str(created["cluster_id"]), "contextId": str(ctx["id"]), "language": "sql", "command": "SELECT 2",
	}, &execd); st != 200 {
		t.Fatalf("sql exec %d %+v", st, execd)
	}
	var cmd map[string]any
	h.json("GET", "/api/1.2/commands/status?commandId="+str(execd["id"]), pat, nil, &cmd)
	if cmd["status"] != "Finished" || !strings.Contains(str(cmd["results"].(map[string]any)["data"]), "2") {
		t.Fatalf("sql result %+v", cmd)
	}
}
