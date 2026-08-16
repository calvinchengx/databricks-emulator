package hs2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

type fakeBackend struct {
	running bool
	ok      bool
	stdout  string
	err     error
}

func (f fakeBackend) Lookup(string) (bool, bool) { return f.running, f.ok }
func (f fakeBackend) Run(string, string) (string, error) {
	return f.stdout, f.err
}

func TestServiceSelect1AndFallbacks(t *testing.T) {
	s := New(fakeBackend{running: true, ok: true, stdout: `[{"1":1}]`})
	ctx := withWarehouse(context.Background(), "wh-1")
	opened, err := s.OpenSession(ctx, &cliservice.TOpenSessionReq{
		ClientProtocolI64: protoI64(cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7),
	})
	if err != nil || opened.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("open %v %+v", err, opened)
	}
	execd, err := s.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle:    opened.SessionHandle,
		Statement:        "SELECT 1",
		GetDirectResults: &cliservice.TSparkGetDirectResults{MaxRows: 10},
	})
	if err != nil || execd.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("exec %v %+v", err, execd)
	}
	if execd.DirectResults.ResultSet.Results.Columns[0].I32Val.Values[0] != 1 {
		t.Fatalf("direct %+v", execd.DirectResults)
	}
	st, err := s.GetOperationStatus(ctx, &cliservice.TGetOperationStatusReq{OperationHandle: execd.OperationHandle})
	if err != nil || st.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("status %v %+v", err, st)
	}
	md, err := s.GetResultSetMetadata(ctx, &cliservice.TGetResultSetMetadataReq{OperationHandle: execd.OperationHandle})
	if err != nil || md.Schema == nil || len(md.Schema.Columns) != 1 {
		t.Fatalf("meta %v %+v", err, md)
	}
	fetched, err := s.FetchResults(ctx, &cliservice.TFetchResultsReq{OperationHandle: execd.OperationHandle})
	if err != nil || fetched.Results.Columns[0].I32Val.Values[0] != 1 {
		t.Fatalf("fetch %v %+v", err, fetched)
	}
	if _, err := s.CloseOperation(ctx, &cliservice.TCloseOperationReq{OperationHandle: execd.OperationHandle}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelOperation(ctx, &cliservice.TCancelOperationReq{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CloseSession(ctx, &cliservice.TCloseSessionReq{SessionHandle: opened.SessionHandle}); err != nil {
		t.Fatal(err)
	}
}

func TestServiceRefusals(t *testing.T) {
	s := New(fakeBackend{ok: false})
	ctx := withWarehouse(context.Background(), "wh-x")
	miss, _ := s.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	if miss.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatalf("missing warehouse %+v", miss)
	}
	old := protoI64(cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V1)
	low, _ := s.OpenSession(ctx, &cliservice.TOpenSessionReq{ClientProtocolI64: old})
	if low.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatalf("old protocol %+v", low)
	}
	empty, _ := s.OpenSession(context.Background(), &cliservice.TOpenSessionReq{})
	if empty.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatalf("no path %+v", empty)
	}

	s = New(fakeBackend{running: false, ok: true})
	ctx = withWarehouse(context.Background(), "wh-1")
	opened, _ := s.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	stopped, _ := s.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle: opened.SessionHandle, Statement: "SELECT 1",
	})
	if !strings.Contains(stopped.GetStatus().GetErrorMessage(), "STOPPED") {
		t.Fatalf("stopped %+v", stopped)
	}

	s = New(fakeBackend{running: true, ok: true, err: errors.New("no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL")})
	opened, _ = s.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	noeng, _ := s.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle: opened.SessionHandle, Statement: "SELECT 1",
		GetDirectResults: &cliservice.TSparkGetDirectResults{MaxRows: 1},
	})
	if !strings.Contains(noeng.GetStatus().GetErrorMessage(), "DATABRICKS_SPARK_CONNECT_URL") {
		t.Fatalf("no engine %+v", noeng)
	}

	s = New(fakeBackend{running: true, ok: true, stdout: "not-json"})
	opened, _ = s.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	bad, _ := s.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle: opened.SessionHandle, Statement: "SELECT 1",
		GetDirectResults: &cliservice.TSparkGetDirectResults{MaxRows: 1},
	})
	if bad.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatalf("unparseable %+v", bad)
	}

	opened, _ = s.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	params, _ := s.ExecuteStatement(ctx, &cliservice.TExecuteStatementReq{
		SessionHandle: opened.SessionHandle,
		Statement:     "SELECT ?",
		Parameters:    []*cliservice.TSparkParameter{{}},
	})
	if !strings.Contains(params.GetStatus().GetErrorMessage(), "parameters") {
		t.Fatalf("params %+v", params)
	}

	if info, _ := s.GetInfo(ctx, &cliservice.TGetInfoReq{}); info.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatalf("GetInfo %+v", info)
	}
	s.GetTypeInfo(ctx, &cliservice.TGetTypeInfoReq{})
	s.GetCatalogs(ctx, &cliservice.TGetCatalogsReq{})
	s.GetTableTypes(ctx, &cliservice.TGetTableTypesReq{})
	s.GetColumns(ctx, &cliservice.TGetColumnsReq{})
	s.GetFunctions(ctx, &cliservice.TGetFunctionsReq{})
	s.GetPrimaryKeys(ctx, &cliservice.TGetPrimaryKeysReq{})
	s.GetCrossReference(ctx, &cliservice.TGetCrossReferenceReq{})
	s.GetDelegationToken(ctx, &cliservice.TGetDelegationTokenReq{})
	s.CancelDelegationToken(ctx, &cliservice.TCancelDelegationTokenReq{})
	s.RenewDelegationToken(ctx, &cliservice.TRenewDelegationTokenReq{})
	s.GetOperationStatus(ctx, nil)
	s.GetResultSetMetadata(ctx, nil)
	s.FetchResults(ctx, nil)
}

func TestServeHTTPOpenSession(t *testing.T) {
	s := New(fakeBackend{running: true, ok: true, stdout: `[[true], [false]]`})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(w, r, "wh-1")
	}))
	t.Cleanup(ts.Close)
	trans, err := thrift.NewTHttpClient(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	httpTrans := trans.(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	factory := thrift.NewTBinaryProtocolFactoryConf(nil)
	client := cliservice.NewTCLIServiceClientFactory(trans, factory)
	opened, err := client.OpenSession(context.Background(), &cliservice.TOpenSessionReq{})
	if err != nil || opened.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("http open %v %+v", err, opened)
	}
}

type scriptBackend struct{}

func (scriptBackend) Lookup(string) (bool, bool) { return true, true }
func (scriptBackend) Run(_ string, sql string) (string, error) {
	u := strings.ToUpper(strings.TrimSpace(sql))
	switch {
	case strings.HasPrefix(u, "SHOW SCHEMAS"):
		return `[{"namespace":"default"}]`, nil
	case strings.HasPrefix(u, "SHOW TABLES"):
		return `[{"namespace":"default","tableName":"one","isTemporary":false}]`, nil
	default:
		return "", errors.New("unexpected sql: " + sql)
	}
}

func TestServiceGetSchemasAndTables(t *testing.T) {
	s := New(scriptBackend{})
	ctx := withWarehouse(context.Background(), "wh-1")
	opened, err := s.OpenSession(ctx, &cliservice.TOpenSessionReq{})
	if err != nil || opened.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("open %v %+v", err, opened)
	}
	direct := &cliservice.TSparkGetDirectResults{MaxRows: 100}
	hive := cliservice.TIdentifier("hive_metastore")
	hivePat := cliservice.TPatternOrIdentifier("hive_metastore")
	def := cliservice.TPatternOrIdentifier("default")
	schemas, err := s.GetSchemas(ctx, &cliservice.TGetSchemasReq{
		SessionHandle:    opened.SessionHandle,
		CatalogName:      &hive,
		GetDirectResults: direct,
	})
	if err != nil || schemas.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("schemas %v %+v", err, schemas)
	}
	if schemas.DirectResults == nil || schemas.DirectResults.ResultSet == nil {
		t.Fatalf("no schema rows %+v", schemas)
	}
	got := schemas.DirectResults.ResultSet.Results.Columns[0].StringVal.Values
	if len(got) != 1 || got[0] != "default" {
		t.Fatalf("schemas %v", got)
	}
	tables, err := s.GetTables(ctx, &cliservice.TGetTablesReq{
		SessionHandle:    opened.SessionHandle,
		CatalogName:      &hivePat,
		SchemaName:       &def,
		GetDirectResults: direct,
	})
	if err != nil || tables.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("tables %v %+v", err, tables)
	}
	names := tables.DirectResults.ResultSet.Results.Columns[2].StringVal.Values
	if len(names) != 1 || names[0] != "one" {
		t.Fatalf("tables %v", names)
	}
	miss, _ := s.GetSchemas(ctx, &cliservice.TGetSchemasReq{})
	if miss.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatalf("missing session %+v", miss)
	}
	if quoteIdent("a`b") != "`a``b`" {
		t.Fatalf("quote %s", quoteIdent("a`b"))
	}
	if !likeMatch("default", "%") || !likeMatch("one", "o%") || likeMatch("one", "two") {
		t.Fatal("like")
	}
	if wantTableType([]string{"VIEW"}, "TABLE") || !wantTableType(nil, "TABLE") {
		t.Fatal("types")
	}
}

func protoI64(v cliservice.TProtocolVersion) *int64 {
	n := int64(v)
	return &n
}

func TestParseStdoutTypes(t *testing.T) {
	tab, err := parseStdout(`[{"ok":true,"n":1.5,"s":"hi","big":2147483648}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(tab.names) != 4 {
		t.Fatalf("names %v", tab.names)
	}
	arrays, err := parseStdout(`[[1,2],[3,4]]`)
	if err != nil || len(arrays.cols) != 2 {
		t.Fatalf("arrays %v %+v", err, arrays)
	}
	empty, err := parseStdout(`[]`)
	if err != nil || empty == nil {
		t.Fatalf("empty %v", err)
	}
	env, err := parseStdout(`{"schema":{"fields":[{"name":"b","type":"boolean"},{"name":"d","type":"double"},{"name":"t","type":"string"}]},"data":[]}`)
	if err != nil || len(env.names) != 3 {
		t.Fatalf("empty envelope %v %+v", err, env)
	}
	bools, err := parseStdout(`{"schema":{"fields":[{"name":"ok","type":"boolean"}]},"data":[[true]]}`)
	if err != nil || bools.cols[0].BoolVal == nil || !bools.cols[0].BoolVal.Values[0] {
		t.Fatalf("bool %v %+v", err, bools)
	}
	dbl, err := parseStdout(`{"schema":{"fields":[{"name":"x","type":"double"}]},"data":[[1.25]]}`)
	if err != nil || dbl.cols[0].DoubleVal == nil || dbl.cols[0].DoubleVal.Values[0] != 1.25 {
		t.Fatalf("double %v %+v", err, dbl)
	}
	str, err := parseStdout(`{"schema":{"fields":[{"name":"s","type":"string"}]},"data":[["hi"]]}`)
	if err != nil || str.cols[0].StringVal == nil || str.cols[0].StringVal.Values[0] != "hi" {
		t.Fatalf("string %v %+v", err, str)
	}
	big, err := parseStdout(`{"schema":{"fields":[{"name":"n","type":"long"}]},"data":[[2147483648]]}`)
	if err != nil || big.cols[0].I64Val == nil || big.cols[0].I64Val.Values[0] != 2147483648 {
		t.Fatalf("bigint %v %+v", err, big)
	}
}
