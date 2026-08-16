package hs2

import (
	"context"
	"fmt"
	"path"
	"strings"
	"unicode"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

func (s *Service) lookupSession(h *cliservice.TSessionHandle) (session, bool) {
	if h == nil {
		return session{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sess[handleKey(h.SessionId)]
	return sess, ok
}

func (s *Service) runParsed(sess session, sql string) (*table, error) {
	if s.Backend == nil {
		return nil, fmt.Errorf("no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL")
	}
	running, found := s.Backend.Lookup(sess.warehouse)
	if !found {
		return nil, fmt.Errorf("warehouse not found")
	}
	if !running {
		return nil, fmt.Errorf("warehouse is STOPPED")
	}
	stdout, err := s.Backend.Run(sess.warehouse, sql)
	if err != nil {
		return nil, err
	}
	return parseStdout(stdout)
}

func (s *Service) finishMeta(tab *table, err error, direct *cliservice.TSparkGetDirectResults, opType cliservice.TOperationType) (string, *cliservice.TStatus, *cliservice.TOperationHandle, *cliservice.TSparkDirectResults) {
	op := newHandle()
	key := handleKey(op)
	handle := &cliservice.TOperationHandle{OperationId: op, OperationType: opType, HasResultSet: true}
	if err != nil {
		s.mu.Lock()
		s.ops[key] = operation{err: err.Error()}
		s.mu.Unlock()
		var dr *cliservice.TSparkDirectResults
		if direct != nil {
			dr = s.directError(key, err.Error())
		}
		return key, errStatus(err.Error()), handle, dr
	}
	if tab == nil {
		tab = emptyTable()
	}
	s.mu.Lock()
	s.ops[key] = operation{tab: tab}
	s.mu.Unlock()
	var dr *cliservice.TSparkDirectResults
	if direct != nil {
		dr = s.directOK(key, tab)
	}
	return key, okStatus(), handle, dr
}

func (s *Service) GetSchemas(_ context.Context, req *cliservice.TGetSchemasReq) (*cliservice.TGetSchemasResp, error) {
	if req == nil || req.SessionHandle == nil {
		return &cliservice.TGetSchemasResp{Status: errStatus("sessionHandle is required")}, nil
	}
	sess, ok := s.lookupSession(req.SessionHandle)
	if !ok {
		return &cliservice.TGetSchemasResp{Status: errStatus("unknown session")}, nil
	}
	cat := strings.TrimSpace(string(req.GetCatalogName()))
	sql := "SHOW SCHEMAS"
	if cat != "" && !isHiveCatalog(cat) {
		sql = "SHOW SCHEMAS IN " + quoteIdent(cat)
	}
	raw, err := s.runParsed(sess, sql)
	if err != nil {
		_, st, h, dr := s.finishMeta(nil, err, req.GetDirectResults, cliservice.TOperationType_GET_SCHEMAS)
		return &cliservice.TGetSchemasResp{Status: st, OperationHandle: h, DirectResults: dr}, nil
	}
	if cat == "" {
		cat = "hive_metastore"
	}
	pat := strings.TrimSpace(string(req.GetSchemaName()))
	var rows [][]string
	for i := 0; i < raw.rowCount(); i++ {
		name := raw.namedCell(i, "namespace", "database", "schemaName")
		if name == "" {
			name = raw.cell(i, 0)
		}
		if name == "" || !likeMatch(name, pat) {
			continue
		}
		rows = append(rows, []string{name, cat})
	}
	tab := stringTable([]string{"TABLE_SCHEM", "TABLE_CATALOG"}, rows)
	_, st, h, dr := s.finishMeta(tab, nil, req.GetDirectResults, cliservice.TOperationType_GET_SCHEMAS)
	return &cliservice.TGetSchemasResp{Status: st, OperationHandle: h, DirectResults: dr}, nil
}

func (s *Service) GetTables(_ context.Context, req *cliservice.TGetTablesReq) (*cliservice.TGetTablesResp, error) {
	if req == nil || req.SessionHandle == nil {
		return &cliservice.TGetTablesResp{Status: errStatus("sessionHandle is required")}, nil
	}
	sess, ok := s.lookupSession(req.SessionHandle)
	if !ok {
		return &cliservice.TGetTablesResp{Status: errStatus("unknown session")}, nil
	}
	cat := strings.TrimSpace(string(req.GetCatalogName()))
	schema := strings.TrimSpace(string(req.GetSchemaName()))
	sql := "SHOW TABLES"
	switch {
	case schema != "" && cat != "" && !isHiveCatalog(cat):
		sql = "SHOW TABLES IN " + quoteIdent(cat) + "." + quoteIdent(schema)
	case schema != "":
		sql = "SHOW TABLES IN " + quoteIdent(schema)
	}
	raw, err := s.runParsed(sess, sql)
	if err != nil {
		_, st, h, dr := s.finishMeta(nil, err, req.GetDirectResults, cliservice.TOperationType_GET_TABLES)
		return &cliservice.TGetTablesResp{Status: st, OperationHandle: h, DirectResults: dr}, nil
	}
	if cat == "" {
		cat = "hive_metastore"
	}
	namePat := strings.TrimSpace(string(req.GetTableName()))
	var rows [][]string
	for i := 0; i < raw.rowCount(); i++ {
		ns := raw.namedCell(i, "namespace", "database")
		name := raw.namedCell(i, "tableName", "table_name", "name")
		if name == "" {
			name = raw.cell(i, 1)
		}
		if name == "" {
			name = raw.cell(i, 0)
		}
		if ns == "" {
			ns = schema
		}
		if schema != "" && ns != "" && !likeMatch(ns, schema) {
			continue
		}
		if !likeMatch(name, namePat) {
			continue
		}
		kind := "TABLE"
		if !wantTableType(req.TableTypes, kind) {
			continue
		}
		rows = append(rows, []string{cat, ns, name, kind, ""})
	}
	tab := stringTable([]string{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"}, rows)
	_, st, h, dr := s.finishMeta(tab, nil, req.GetDirectResults, cliservice.TOperationType_GET_TABLES)
	return &cliservice.TGetTablesResp{Status: st, OperationHandle: h, DirectResults: dr}, nil
}

func isHiveCatalog(name string) bool {
	return strings.EqualFold(name, "hive_metastore")
}

func quoteIdent(name string) string {
	var b strings.Builder
	b.WriteByte('`')
	for _, r := range name {
		if r == '`' {
			b.WriteString("``")
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		// Refuse injection: drop anything that is not an identifier rune.
	}
	b.WriteByte('`')
	return b.String()
}

func likeMatch(value, pattern string) bool {
	if pattern == "" || pattern == "%" {
		return true
	}
	ok, err := path.Match(likeToGlob(pattern), value)
	return err == nil && ok
}

func likeToGlob(pattern string) string {
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteByte('*')
		case '_':
			b.WriteByte('?')
		case '*', '?', '[', '\\':
			b.WriteByte('\\')
			b.WriteByte(pattern[i])
		default:
			b.WriteByte(pattern[i])
		}
	}
	return b.String()
}

func wantTableType(types []string, kind string) bool {
	if len(types) == 0 {
		return true
	}
	for _, t := range types {
		if strings.EqualFold(t, kind) {
			return true
		}
	}
	return false
}
