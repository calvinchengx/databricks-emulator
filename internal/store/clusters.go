package store

import (
	"fmt"
	"sync"
)

// Cluster is a session handle onto the attached Spark engine, not a VM.
type Cluster struct {
	ID           string
	Name         string
	SparkVersion string
	NodeTypeID   string
	NumWorkers   int
	State        string
	StateMessage string
	Creator      string
	PolicyID     string
}

// Clusters holds session handles.
type Clusters struct {
	mu     sync.Mutex
	nextID int64
	all    map[string]*Cluster
}

func newClusters() *Clusters {
	return &Clusters{all: map[string]*Cluster{}}
}

// Create inserts a PENDING handle. The HTTP layer starts the Sail session
// and then marks it RUNNING — a timer must not do that.
func (c *Clusters) Create(name, sparkVersion, nodeType string, workers int, creator, policyID string) *Cluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	cl := &Cluster{
		ID:           fmt.Sprintf("cluster-%d", c.nextID),
		Name:         name,
		SparkVersion: sparkVersion,
		NodeTypeID:   nodeType,
		NumWorkers:   workers,
		State:        "PENDING",
		StateMessage: "starting a session on the attached Spark engine",
		Creator:      creator,
		PolicyID:     policyID,
	}
	c.all[cl.ID] = cl
	return cl
}

// Get returns a cluster.
func (c *Clusters) Get(id string) (*Cluster, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.all[id]
	return cl, ok
}

// List returns every cluster.
func (c *Clusters) List() []*Cluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Cluster, 0, len(c.all))
	for _, cl := range c.all {
		out = append(out, cl)
	}
	return out
}

// SetState records a lifecycle change.
func (c *Clusters) SetState(id, state, message string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.all[id]
	if !ok {
		return false
	}
	cl.State = state
	cl.StateMessage = message
	return true
}

// Delete removes a cluster.
func (c *Clusters) Delete(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.all[id]; !ok {
		return false
	}
	delete(c.all, id)
	return true
}
