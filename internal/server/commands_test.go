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

func TestCommandExecutionRefusalsAndEngineErrors(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.1/clusters/create", pat, map[string]any{"cluster_name": "doors"}, &created); st != 200 {
		t.Fatalf("cluster %d", st)
	}
	clusterID := str(created["cluster_id"])

	if st := h.json("POST", "/api/1.2/contexts/create", pat, "{", nil); st != 400 {
		t.Fatalf("bad create json %d", st)
	}
	if st := h.json("POST", "/api/1.2/contexts/destroy", pat, "{", nil); st != 400 {
		t.Fatalf("bad destroy json %d", st)
	}
	if st := h.json("POST", "/api/1.2/commands/execute", pat, "{", nil); st != 400 {
		t.Fatalf("bad execute json %d", st)
	}
	if st := h.json("POST", "/api/1.2/commands/cancel", pat, "{", nil); st != 400 {
		t.Fatalf("bad cancel json %d", st)
	}
	if st := h.json("POST", "/api/1.2/contexts/destroy", pat, map[string]any{"contextId": "nope"}, nil); st != 404 {
		t.Fatalf("destroy missing %d", st)
	}
	if st := h.json("GET", "/api/1.2/commands/status?commandId=nope", pat, nil, nil); st != 404 {
		t.Fatalf("status missing %d", st)
	}
	if st := h.json("POST", "/api/1.2/commands/cancel", pat, map[string]any{"commandId": "nope"}, nil); st != 404 {
		t.Fatalf("cancel missing %d", st)
	}

	var body map[string]any
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "r",
	}, &body); st != 501 || !strings.Contains(str(body["message"]), "r") {
		t.Fatalf("r %d %+v", st, body)
	}
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "haskell",
	}, &body); st != 501 {
		t.Fatalf("unknown lang %d %+v", st, body)
	}

	h.srv.Store.Clusters.SetState(clusterID, "TERMINATED", "stopped")
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "python",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "RUNNING") {
		t.Fatalf("not running %d %+v", st, body)
	}
	h.srv.Store.Clusters.SetState(clusterID, "RUNNING", "session handle")

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{}, errString("agent down")
	}
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID,
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "agent down") {
		t.Fatalf("ping err %d %+v", st, body)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EValue: "probe failed"}, nil
	}
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "python",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "probe failed") {
		t.Fatalf("ping evalue %d %+v", st, body)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EName: "NameError"}, nil
	}
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "python",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "NameError") {
		t.Fatalf("ping ename %d %+v", st, body)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false}, nil
	}
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID, "language": "python",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "context failed to start") {
		t.Fatalf("ping empty %d %+v", st, body)
	}

	h.exec.Hook = nil
	var ctx map[string]any
	if st := h.json("POST", "/api/1.2/contexts/create", pat, map[string]any{
		"clusterId": clusterID,
	}, &ctx); st != 200 {
		t.Fatalf("default python %d %+v", st, ctx)
	}
	ctxID := str(ctx["id"])

	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "command": "   ",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "command is required") {
		t.Fatalf("empty command %d %+v", st, body)
	}
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "language": "scala", "command": "1",
	}, &body); st != 501 {
		t.Fatalf("exec scala %d %+v", st, body)
	}
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": "nope", "command": "print(1)",
	}, &body); st != 404 {
		t.Fatalf("exec missing ctx %d", st)
	}
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"clusterId": "other", "contextId": ctxID, "command": "print(1)",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "clusterId") {
		t.Fatalf("cluster mismatch %d %+v", st, body)
	}

	pending := h.srv.Store.Commands.CreateContext(clusterID, "python")
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": pending.ID, "command": "print(1)",
	}, &body); st != 400 || !strings.Contains(str(body["message"]), "not Running") {
		t.Fatalf("pending ctx %d %+v", st, body)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{}, errString("run failed")
	}
	var execd map[string]any
	if st := h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "command": "print(1)",
	}, &execd); st != 200 {
		t.Fatalf("exec err %d %+v", st, execd)
	}
	var cmd map[string]any
	h.json("GET", "/api/1.2/commands/status?commandId="+str(execd["id"]), pat, nil, &cmd)
	if cmd["status"] != "Error" || !strings.Contains(str(cmd["results"].(map[string]any)["summary"]), "run failed") {
		t.Fatalf("run err %+v", cmd)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EValue: "boom", EName: "ValueError"}, nil
	}
	h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "command": "raise",
	}, &execd)
	h.json("GET", "/api/1.2/commands/status?commandId="+str(execd["id"]), pat, nil, &cmd)
	results := cmd["results"].(map[string]any)
	if cmd["status"] != "Error" || results["summary"] != "boom" || results["cause"] != "ValueError" {
		t.Fatalf("evalue %+v", cmd)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EName: "OnlyName"}, nil
	}
	h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "command": "raise2",
	}, &execd)
	h.json("GET", "/api/1.2/commands/status?commandId="+str(execd["id"]), pat, nil, &cmd)
	if str(cmd["results"].(map[string]any)["summary"]) != "OnlyName" {
		t.Fatalf("ename only %+v", cmd)
	}

	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false}, nil
	}
	h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "command": "raise3",
	}, &execd)
	h.json("GET", "/api/1.2/commands/status?commandId="+str(execd["id"]), pat, nil, &cmd)
	if str(cmd["results"].(map[string]any)["summary"]) != "command failed" {
		t.Fatalf("empty fail %+v", cmd)
	}

	h.srv.Spark = nil
	h.json("POST", "/api/1.2/commands/execute", pat, map[string]any{
		"contextId": ctxID, "command": "print(1)",
	}, &execd)
	h.json("GET", "/api/1.2/commands/status?commandId="+str(execd["id"]), pat, nil, &cmd)
	if cmd["status"] != "Error" || !strings.Contains(str(cmd["results"].(map[string]any)["summary"]), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("nil spark exec %+v", cmd)
	}
}
