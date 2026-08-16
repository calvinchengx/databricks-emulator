package store

import (
	"fmt"
	"strings"
	"sync"
)

// Warehouse is a session handle onto the attached Spark engine, not a VM.
type Warehouse struct {
	ID          string
	Name        string
	ClusterSize string
	State       string
}

// Statement is one Spark SQL execution.
type Statement struct {
	ID          string
	WarehouseID string
	SQL         string
	Status      string
	Error       string
	Stdout      string
	Dialect     string
	ExecutedBy  string
	UserName    string
}

// Query is a saved Databricks SQL query object.
type Query struct {
	ID                   string
	DisplayName          string
	QueryText            string
	WarehouseID          string
	Description          string
	Catalog              string
	Schema               string
	ParentPath           string
	Tags                 []string
	Parameters           []any
	ApplyAutoLimit       bool
	RunAsMode            string
	LifecycleState       string
	CreateTime           string
	UpdateTime           string
	OwnerUserName        string
	LastModifierUserName string
}

// QueryHistory is one warehouse execution, newest first in the list.
type QueryHistory struct {
	QueryID            string
	WarehouseID        string
	QueryText          string
	Status             string
	ErrorMessage       string
	QueryStartTimeMs   int64
	QueryEndTimeMs     int64
	ExecutionEndTimeMs int64
	Duration           int64
	IsFinal            bool
	UserName           string
	StatementType      string
}

// HistoryFilter narrows ListHistory. Empty slices mean "any".
type HistoryFilter struct {
	WarehouseIDs []string
	Statuses     []string
	StatementIDs []string
	StartTimeMs  int64
	EndTimeMs    int64
}

// SQL holds warehouses, statements, saved queries, and query history.
type SQL struct {
	mu      sync.Mutex
	nextID  int64
	wh      map[string]*Warehouse
	stmt    map[string]*Statement
	queries map[string]*Query
	history []*QueryHistory
}

func newSQL() *SQL {
	return &SQL{wh: map[string]*Warehouse{}, stmt: map[string]*Statement{}, queries: map[string]*Query{}}
}

// CreateWarehouse inserts a RUNNING session handle.
func (s *SQL) CreateWarehouse(name, size string) *Warehouse {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	w := &Warehouse{
		ID:          fmt.Sprintf("wh-%d", s.nextID),
		Name:        name,
		ClusterSize: size,
		State:       "RUNNING",
	}
	s.wh[w.ID] = w
	return w
}

// GetWarehouse returns a warehouse.
func (s *SQL) GetWarehouse(id string) (*Warehouse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.wh[id]
	return w, ok
}

// FirstRunning returns one RUNNING warehouse, if any.
func (s *SQL) FirstRunning() (*Warehouse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.wh {
		if w.State == "RUNNING" {
			return w, true
		}
	}
	return nil, false
}

// ListWarehouses returns every warehouse.
func (s *SQL) ListWarehouses() []*Warehouse {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Warehouse
	for _, w := range s.wh {
		out = append(out, w)
	}
	return out
}

// DeleteWarehouse removes a warehouse.
func (s *SQL) DeleteWarehouse(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.wh[id]; !ok {
		return false
	}
	delete(s.wh, id)
	return true
}

// SetWarehouseState starts or stops the handle.
func (s *SQL) SetWarehouseState(id, state string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.wh[id]
	if !ok {
		return false
	}
	w.State = state
	return true
}

// NewStatement records a statement.
func (s *SQL) NewStatement(warehouseID, sql string) *Statement {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	st := &Statement{
		ID:          fmt.Sprintf("stmt-%d", s.nextID),
		WarehouseID: warehouseID,
		SQL:         sql,
		Status:      "PENDING",
		Dialect:     "spark-sql",
		ExecutedBy:  "the emulator's Spark engine (Spark SQL), not Photon",
	}
	s.stmt[st.ID] = st
	return st
}

// GetStatement returns a statement.
func (s *SQL) GetStatement(id string) (*Statement, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.stmt[id]
	return st, ok
}

// UpdateStatement stores a finished or in-flight statement.
func (s *SQL) UpdateStatement(st *Statement) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stmt[st.ID] = st
}

// CreateQuery inserts an ACTIVE saved query.
func (s *SQL) CreateQuery(q *Query) *Query {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	q.ID = fmt.Sprintf("qry-%d", s.nextID)
	if q.LifecycleState == "" {
		q.LifecycleState = "ACTIVE"
	}
	if q.RunAsMode == "" {
		q.RunAsMode = "OWNER"
	}
	s.queries[q.ID] = q
	return q
}

// GetQuery returns a saved query, including trashed ones.
func (s *SQL) GetQuery(id string) (*Query, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queries[id]
	return q, ok
}

// ListQueries returns ACTIVE queries in creation order.
func (s *SQL) ListQueries() []*Query {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Query
	for i := int64(1); i <= s.nextID; i++ {
		q, ok := s.queries[fmt.Sprintf("qry-%d", i)]
		if ok && q.LifecycleState == "ACTIVE" {
			out = append(out, q)
		}
	}
	return out
}

// UpdateQuery stores an edited query.
func (s *SQL) UpdateQuery(q *Query) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries[q.ID] = q
}

// TrashQuery marks a query TRASHED. Already-trashed or missing is false.
func (s *SQL) TrashQuery(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queries[id]
	if !ok || q.LifecycleState == "TRASHED" {
		return false
	}
	q.LifecycleState = "TRASHED"
	return true
}

// DisplayNameTaken reports an ACTIVE query with this display name, optionally excluding one id.
func (s *SQL) DisplayNameTaken(name, exceptID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queries {
		if q.LifecycleState == "ACTIVE" && q.DisplayName == name && q.ID != exceptID {
			return true
		}
	}
	return false
}

// ResolveDisplayName returns name, or name with a numeric suffix when auto-resolving a clash.
func (s *SQL) ResolveDisplayName(name string, auto bool, exceptID string) (string, bool) {
	if !s.DisplayNameTaken(name, exceptID) {
		return name, true
	}
	if !auto {
		return "", false
	}
	for n := 1; n < 1000; n++ {
		cand := fmt.Sprintf("%s (%d)", name, n)
		if !s.DisplayNameTaken(cand, exceptID) {
			return cand, true
		}
	}
	return "", false
}

// RecordHistory prepends one warehouse execution.
func (s *SQL) RecordHistory(h *QueryHistory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = append([]*QueryHistory{h}, s.history...)
}

// ListHistory returns matching executions, newest first, then a page of size limit from offset.
func (s *SQL) ListHistory(f HistoryFilter, offset, limit int) (items []*QueryHistory, nextOffset int, hasNext bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var matched []*QueryHistory
	for _, h := range s.history {
		if historyMatches(h, f) {
			matched = append(matched, h)
		}
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	items = matched[offset:end]
	if end < len(matched) {
		return items, end, true
	}
	return items, 0, false
}

func historyMatches(h *QueryHistory, f HistoryFilter) bool {
	if len(f.WarehouseIDs) > 0 && !containsFold(f.WarehouseIDs, h.WarehouseID) {
		return false
	}
	if len(f.Statuses) > 0 && !containsFold(f.Statuses, h.Status) {
		return false
	}
	if len(f.StatementIDs) > 0 && !containsFold(f.StatementIDs, h.QueryID) {
		return false
	}
	if f.StartTimeMs > 0 && h.QueryStartTimeMs < f.StartTimeMs {
		return false
	}
	if f.EndTimeMs > 0 && h.QueryStartTimeMs > f.EndTimeMs {
		return false
	}
	return true
}

func containsFold(have []string, want string) bool {
	for _, v := range have {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}
