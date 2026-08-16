package hs2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

// Backend is the warehouse + Spark attach this process already owns.
type Backend interface {
	Lookup(id string) (running bool, ok bool)
	Run(warehouseID, sql string) (stdout string, err error)
}

type session struct {
	warehouse string
}

type operation struct {
	tab *table
	err string
}

// Service is the HiveServer2 surface. Sessions bind to the warehouse id
// taken from the HTTP path, not from a Thrift field.
type Service struct {
	Backend Backend

	mu   sync.Mutex
	sess map[string]session
	ops  map[string]operation
}

func New(b Backend) *Service {
	return &Service{
		Backend: b,
		sess:    map[string]session{},
		ops:     map[string]operation{},
	}
}

type ctxKey int

const warehouseKey ctxKey = 1

func withWarehouse(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, warehouseKey, id)
}

func warehouseOf(ctx context.Context) string {
	id, _ := ctx.Value(warehouseKey).(string)
	return id
}

func okStatus() *cliservice.TStatus {
	return &cliservice.TStatus{StatusCode: cliservice.TStatusCode_SUCCESS_STATUS}
}

func errStatus(msg string) *cliservice.TStatus {
	return &cliservice.TStatus{
		StatusCode:   cliservice.TStatusCode_ERROR_STATUS,
		ErrorMessage: &msg,
	}
}

func newHandle() *cliservice.THandleIdentifier {
	guid := make([]byte, 16)
	secret := make([]byte, 16)
	_, _ = rand.Read(guid)
	_, _ = rand.Read(secret)
	return &cliservice.THandleIdentifier{GUID: guid, Secret: secret}
}

func handleKey(h *cliservice.THandleIdentifier) string {
	if h == nil {
		return ""
	}
	return hex.EncodeToString(h.GUID)
}

func (s *Service) OpenSession(ctx context.Context, req *cliservice.TOpenSessionReq) (*cliservice.TOpenSessionResp, error) {
	id := warehouseOf(ctx)
	ver := cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7
	if req != nil && req.IsSetClientProtocolI64() {
		got := cliservice.TProtocolVersion(req.GetClientProtocolI64())
		if got >= cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V1 &&
			got < cliservice.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V2 {
			return &cliservice.TOpenSessionResp{
				Status:                errStatus("serverProtocolVersion must be SPARK_CLI_SERVICE_PROTOCOL_V2 or newer"),
				ServerProtocolVersion: ver,
			}, nil
		}
	}
	if id == "" {
		return &cliservice.TOpenSessionResp{
			Status:                errStatus("warehouse id missing from HTTP path"),
			ServerProtocolVersion: ver,
		}, nil
	}
	if s.Backend == nil {
		return &cliservice.TOpenSessionResp{
			Status:                errStatus("no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL"),
			ServerProtocolVersion: ver,
		}, nil
	}
	if _, ok := s.Backend.Lookup(id); !ok {
		return &cliservice.TOpenSessionResp{
			Status:                errStatus("warehouse not found"),
			ServerProtocolVersion: ver,
		}, nil
	}
	h := newHandle()
	s.mu.Lock()
	s.sess[handleKey(h)] = session{warehouse: id}
	s.mu.Unlock()
	can := true
	return &cliservice.TOpenSessionResp{
		Status:                 okStatus(),
		ServerProtocolVersion:  ver,
		SessionHandle:          &cliservice.TSessionHandle{SessionId: h},
		CanUseMultipleCatalogs: &can,
	}, nil
}

func (s *Service) CloseSession(_ context.Context, req *cliservice.TCloseSessionReq) (*cliservice.TCloseSessionResp, error) {
	if req != nil && req.SessionHandle != nil {
		s.mu.Lock()
		delete(s.sess, handleKey(req.SessionHandle.SessionId))
		s.mu.Unlock()
	}
	return &cliservice.TCloseSessionResp{Status: okStatus()}, nil
}

func (s *Service) ExecuteStatement(ctx context.Context, req *cliservice.TExecuteStatementReq) (*cliservice.TExecuteStatementResp, error) {
	if req == nil || req.SessionHandle == nil {
		return &cliservice.TExecuteStatementResp{Status: errStatus("sessionHandle is required")}, nil
	}
	if len(req.Parameters) > 0 {
		return &cliservice.TExecuteStatementResp{Status: errStatus("native parameters are not implemented")}, nil
	}
	s.mu.Lock()
	sess, ok := s.sess[handleKey(req.SessionHandle.SessionId)]
	s.mu.Unlock()
	if !ok {
		return &cliservice.TExecuteStatementResp{Status: errStatus("unknown session")}, nil
	}
	running, found := s.Backend.Lookup(sess.warehouse)
	if !found {
		return &cliservice.TExecuteStatementResp{Status: errStatus("warehouse not found")}, nil
	}
	if !running {
		return &cliservice.TExecuteStatementResp{Status: errStatus("warehouse is STOPPED")}, nil
	}
	stdout, err := s.Backend.Run(sess.warehouse, req.Statement)
	op := newHandle()
	key := handleKey(op)
	if err != nil {
		s.mu.Lock()
		s.ops[key] = operation{err: err.Error()}
		s.mu.Unlock()
		resp := &cliservice.TExecuteStatementResp{
			Status: errStatus(err.Error()),
			OperationHandle: &cliservice.TOperationHandle{
				OperationId:   op,
				OperationType: cliservice.TOperationType_EXECUTE_STATEMENT,
			},
		}
		if req.GetDirectResults != nil {
			resp.DirectResults = s.directError(key, err.Error())
		}
		return resp, nil
	}
	tab, perr := parseStdout(stdout)
	if perr != nil {
		s.mu.Lock()
		s.ops[key] = operation{err: perr.Error()}
		s.mu.Unlock()
		resp := &cliservice.TExecuteStatementResp{
			Status: errStatus(perr.Error()),
			OperationHandle: &cliservice.TOperationHandle{
				OperationId:   op,
				OperationType: cliservice.TOperationType_EXECUTE_STATEMENT,
			},
		}
		if req.GetDirectResults != nil {
			resp.DirectResults = s.directError(key, perr.Error())
		}
		return resp, nil
	}
	s.mu.Lock()
	s.ops[key] = operation{tab: tab}
	s.mu.Unlock()
	handle := &cliservice.TOperationHandle{
		OperationId:   op,
		OperationType: cliservice.TOperationType_EXECUTE_STATEMENT,
		HasResultSet:  true,
	}
	resp := &cliservice.TExecuteStatementResp{Status: okStatus(), OperationHandle: handle}
	if req.GetDirectResults != nil {
		resp.DirectResults = s.directOK(key, tab)
	}
	return resp, nil
}

func (s *Service) directOK(key string, tab *table) *cliservice.TSparkDirectResults {
	finished := cliservice.TOperationState_FINISHED_STATE
	has := true
	more := false
	format := cliservice.TSparkRowSetType_COLUMN_BASED_SET
	lz4 := false
	return &cliservice.TSparkDirectResults{
		OperationStatus: &cliservice.TGetOperationStatusResp{
			Status:         okStatus(),
			OperationState: &finished,
			HasResultSet:   &has,
		},
		ResultSetMetadata: &cliservice.TGetResultSetMetadataResp{
			Status:        okStatus(),
			Schema:        tab.schema(),
			ResultFormat:  &format,
			Lz4Compressed: &lz4,
		},
		ResultSet: &cliservice.TFetchResultsResp{
			Status:      okStatus(),
			HasMoreRows: &more,
			Results:     tab.rowSet(),
		},
	}
}

func (s *Service) directError(_ string, msg string) *cliservice.TSparkDirectResults {
	st := cliservice.TOperationState_ERROR_STATE
	return &cliservice.TSparkDirectResults{
		OperationStatus: &cliservice.TGetOperationStatusResp{
			Status:         errStatus(msg),
			OperationState: &st,
			ErrorMessage:   &msg,
		},
	}
}

func (s *Service) GetOperationStatus(_ context.Context, req *cliservice.TGetOperationStatusReq) (*cliservice.TGetOperationStatusResp, error) {
	if req == nil || req.OperationHandle == nil {
		return &cliservice.TGetOperationStatusResp{Status: errStatus("operationHandle is required")}, nil
	}
	s.mu.Lock()
	op, ok := s.ops[handleKey(req.OperationHandle.OperationId)]
	s.mu.Unlock()
	if !ok {
		return &cliservice.TGetOperationStatusResp{Status: errStatus("unknown operation")}, nil
	}
	if op.err != "" {
		st := cliservice.TOperationState_ERROR_STATE
		return &cliservice.TGetOperationStatusResp{
			Status:         errStatus(op.err),
			OperationState: &st,
			ErrorMessage:   &op.err,
		}, nil
	}
	finished := cliservice.TOperationState_FINISHED_STATE
	has := true
	return &cliservice.TGetOperationStatusResp{
		Status:         okStatus(),
		OperationState: &finished,
		HasResultSet:   &has,
	}, nil
}

func (s *Service) GetResultSetMetadata(_ context.Context, req *cliservice.TGetResultSetMetadataReq) (*cliservice.TGetResultSetMetadataResp, error) {
	var h *cliservice.TOperationHandle
	if req != nil {
		h = req.GetOperationHandle()
	}
	tab, errMsg, ok := s.opTable(h)
	if !ok {
		return &cliservice.TGetResultSetMetadataResp{Status: errStatus(errMsg)}, nil
	}
	format := cliservice.TSparkRowSetType_COLUMN_BASED_SET
	lz4 := false
	return &cliservice.TGetResultSetMetadataResp{
		Status:        okStatus(),
		Schema:        tab.schema(),
		ResultFormat:  &format,
		Lz4Compressed: &lz4,
	}, nil
}

func (s *Service) FetchResults(_ context.Context, req *cliservice.TFetchResultsReq) (*cliservice.TFetchResultsResp, error) {
	if req == nil {
		return &cliservice.TFetchResultsResp{Status: errStatus("operationHandle is required")}, nil
	}
	tab, errMsg, ok := s.opTable(req.OperationHandle)
	if !ok {
		return &cliservice.TFetchResultsResp{Status: errStatus(errMsg)}, nil
	}
	more := false
	return &cliservice.TFetchResultsResp{
		Status:      okStatus(),
		HasMoreRows: &more,
		Results:     tab.rowSet(),
	}, nil
}

func (s *Service) opTable(h *cliservice.TOperationHandle) (*table, string, bool) {
	if h == nil {
		return nil, "operationHandle is required", false
	}
	s.mu.Lock()
	op, ok := s.ops[handleKey(h.OperationId)]
	s.mu.Unlock()
	if !ok {
		return nil, "unknown operation", false
	}
	if op.err != "" {
		return nil, op.err, false
	}
	if op.tab == nil {
		return emptyTable(), "", true
	}
	return op.tab, "", true
}

func (s *Service) CloseOperation(_ context.Context, req *cliservice.TCloseOperationReq) (*cliservice.TCloseOperationResp, error) {
	if req != nil && req.OperationHandle != nil {
		s.mu.Lock()
		delete(s.ops, handleKey(req.OperationHandle.OperationId))
		s.mu.Unlock()
	}
	return &cliservice.TCloseOperationResp{Status: okStatus()}, nil
}

func (s *Service) CancelOperation(_ context.Context, req *cliservice.TCancelOperationReq) (*cliservice.TCancelOperationResp, error) {
	if req != nil && req.OperationHandle != nil {
		s.mu.Lock()
		delete(s.ops, handleKey(req.OperationHandle.OperationId))
		s.mu.Unlock()
	}
	return &cliservice.TCancelOperationResp{Status: okStatus()}, nil
}

func refused(name string) *cliservice.TStatus {
	return errStatus(fmt.Sprintf("%s is not implemented", name))
}

func (s *Service) GetInfo(context.Context, *cliservice.TGetInfoReq) (*cliservice.TGetInfoResp, error) {
	return &cliservice.TGetInfoResp{Status: refused("GetInfo")}, nil
}
func (s *Service) GetTypeInfo(context.Context, *cliservice.TGetTypeInfoReq) (*cliservice.TGetTypeInfoResp, error) {
	return &cliservice.TGetTypeInfoResp{Status: refused("GetTypeInfo")}, nil
}
func (s *Service) GetCatalogs(context.Context, *cliservice.TGetCatalogsReq) (*cliservice.TGetCatalogsResp, error) {
	return &cliservice.TGetCatalogsResp{Status: refused("GetCatalogs")}, nil
}
func (s *Service) GetTableTypes(context.Context, *cliservice.TGetTableTypesReq) (*cliservice.TGetTableTypesResp, error) {
	return &cliservice.TGetTableTypesResp{Status: refused("GetTableTypes")}, nil
}
func (s *Service) GetColumns(context.Context, *cliservice.TGetColumnsReq) (*cliservice.TGetColumnsResp, error) {
	return &cliservice.TGetColumnsResp{Status: refused("GetColumns")}, nil
}
func (s *Service) GetFunctions(context.Context, *cliservice.TGetFunctionsReq) (*cliservice.TGetFunctionsResp, error) {
	return &cliservice.TGetFunctionsResp{Status: refused("GetFunctions")}, nil
}
func (s *Service) GetPrimaryKeys(context.Context, *cliservice.TGetPrimaryKeysReq) (*cliservice.TGetPrimaryKeysResp, error) {
	return &cliservice.TGetPrimaryKeysResp{Status: refused("GetPrimaryKeys")}, nil
}
func (s *Service) GetCrossReference(context.Context, *cliservice.TGetCrossReferenceReq) (*cliservice.TGetCrossReferenceResp, error) {
	return &cliservice.TGetCrossReferenceResp{Status: refused("GetCrossReference")}, nil
}
func (s *Service) GetDelegationToken(context.Context, *cliservice.TGetDelegationTokenReq) (*cliservice.TGetDelegationTokenResp, error) {
	return &cliservice.TGetDelegationTokenResp{Status: refused("GetDelegationToken")}, nil
}
func (s *Service) CancelDelegationToken(context.Context, *cliservice.TCancelDelegationTokenReq) (*cliservice.TCancelDelegationTokenResp, error) {
	return &cliservice.TCancelDelegationTokenResp{Status: refused("CancelDelegationToken")}, nil
}
func (s *Service) RenewDelegationToken(context.Context, *cliservice.TRenewDelegationTokenReq) (*cliservice.TRenewDelegationTokenResp, error) {
	return &cliservice.TRenewDelegationTokenResp{Status: refused("RenewDelegationToken")}, nil
}
