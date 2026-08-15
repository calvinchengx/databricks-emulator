package store

import (
	"fmt"
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
}

// SQL holds warehouses and statements.
type SQL struct {
	mu     sync.Mutex
	nextID int64
	wh     map[string]*Warehouse
	stmt   map[string]*Statement
}

func newSQL() *SQL {
	return &SQL{wh: map[string]*Warehouse{}, stmt: map[string]*Statement{}}
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
