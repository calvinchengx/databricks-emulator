package server

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

func TestThriftWarehouseSelect1AndRefusals(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "thrift"}, &created); st != 200 {
		t.Fatalf("create %d", st)
	}
	id := str(created["id"])
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		if req.Kind != "sql" || req.Code != "SELECT 1" {
			t.Fatalf("engine request %+v", req)
		}
		return spark.Result{OK: true, Stdout: `[{"1":1}]`}, nil
	}

	cli := thriftClient(t, h, "/sql/1.0/endpoints/"+id, pat)
	ctx := context.Background()
	opened, err := cli.OpenSession(ctx, &cliservice.TOpenSessionReq{
		ClientProtocolI64: protoI64(cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if opened.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS || opened.SessionHandle == nil {
		t.Fatalf("open %+v", opened)
	}
	if opened.ServerProtocolVersion != cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7 {
		t.Fatalf("protocol %v", opened.ServerProtocolVersion)
	}

	direct := &cliservice.TSparkGetDirectResults{MaxRows: 1000}
	execd, err := cli.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle:    opened.SessionHandle,
		Statement:        "SELECT 1",
		GetDirectResults: direct,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execd.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("execute %+v", execd)
	}
	if execd.DirectResults == nil || execd.DirectResults.ResultSet == nil || execd.DirectResults.ResultSet.Results == nil {
		t.Fatalf("no direct results %+v", execd)
	}
	cols := execd.DirectResults.ResultSet.Results.Columns
	if len(cols) != 1 || cols[0].I32Val == nil || len(cols[0].I32Val.Values) != 1 || cols[0].I32Val.Values[0] != 1 {
		t.Fatalf("columns %+v", cols)
	}
	if _, err := cli.CloseOperation(ctx, &cliservice.TCloseOperationReq{OperationHandle: execd.OperationHandle}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.CloseSession(ctx, &cliservice.TCloseSessionReq{SessionHandle: opened.SessionHandle}); err != nil {
		t.Fatal(err)
	}

	bad := h.do("POST", "/sql/1.0/endpoints/"+id, "dev", []byte{0})
	defer bad.Body.Close()
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("token=dev %d", bad.StatusCode)
	}

	unknown := thriftClient(t, h, "/sql/1.0/endpoints/wh-missing", pat)
	miss, err := unknown.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	if err != nil {
		t.Fatal(err)
	}
	if miss.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS || !strings.Contains(miss.GetStatus().GetErrorMessage(), "warehouse not found") {
		t.Fatalf("unknown warehouse %+v", miss)
	}

	if st := h.json("POST", "/api/2.0/sql/warehouses/"+id+"/stop", pat, map[string]any{}, nil); st != 200 {
		t.Fatalf("stop %d", st)
	}
	stoppedOpen, err := cli.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := cli.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle: stoppedOpen.SessionHandle,
		Statement:     "SELECT 1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS || !strings.Contains(stopped.GetStatus().GetErrorMessage(), "STOPPED") {
		t.Fatalf("stopped %+v", stopped)
	}

	h.srv.Spark = nil
	if st := h.json("POST", "/api/2.0/sql/warehouses/"+id+"/start", pat, map[string]any{}, nil); st != 200 {
		t.Fatalf("start %d", st)
	}
	noEngOpen, err := cli.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	if err != nil {
		t.Fatal(err)
	}
	noEng, err := cli.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle: noEngOpen.SessionHandle,
		Statement:     "SELECT 1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if noEng.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS ||
		!strings.Contains(noEng.GetStatus().GetErrorMessage(), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("no engine %+v", noEng)
	}
}

func protoI64(v cliservice.TProtocolVersion) *int64 {
	n := int64(v)
	return &n
}

func thriftClient(t *testing.T, h *harness, path, token string) *cliservice.TCLIServiceClient {
	t.Helper()
	trans, err := thrift.NewTHttpClient(h.http.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	httpTrans := trans.(*thrift.THttpClient)
	httpTrans.SetHeader("Authorization", "Bearer "+token)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	factory := thrift.NewTBinaryProtocolFactoryConf(nil)
	return cliservice.NewTCLIServiceClientFactory(trans, factory)
}

func TestThriftProtocolV1Path(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/sql/warehouses", pat, map[string]any{"name": "p1"}, &created)
	id := str(created["id"])
	h.exec.Hook = func(spark.Request) (spark.Result, error) {
		return spark.Result{OK: true, Stdout: `{"schema":{"fields":[{"name":"1","type":"integer"}]},"data":[[1]]}`}, nil
	}
	cli := thriftClient(t, h, "/sql/protocolv1/o/1/"+id, pat)
	ctx := context.Background()
	opened, err := cli.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	if err != nil || opened.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("open %v %+v", err, opened)
	}
	execd, err := cli.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle:    opened.SessionHandle,
		Statement:        "SELECT 1",
		GetDirectResults: &cliservice.TSparkGetDirectResults{MaxRows: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	cols := execd.DirectResults.ResultSet.Results.Columns
	if len(cols) != 1 || cols[0].I32Val.Values[0] != 1 {
		t.Fatalf("columns %+v", cols)
	}
}
