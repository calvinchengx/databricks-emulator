package store

import (
	"fmt"
	"sync"
)

// ExecContext is a Command Execution REPL on a RUNNING cluster handle.
type ExecContext struct {
	ID        string
	ClusterID string
	Language  string
	Status    string
}

// ExecCommand is one run inside a context.
type ExecCommand struct {
	ID         string
	ContextID  string
	ClusterID  string
	Language   string
	Code       string
	Status     string
	ResultType string
	Data       string
	Summary    string
	Cause      string
}

// Commands holds execution contexts and their commands.
type Commands struct {
	mu       sync.Mutex
	nextID   int64
	contexts map[string]*ExecContext
	cmds     map[string]*ExecCommand
}

func newCommands() *Commands {
	return &Commands{contexts: map[string]*ExecContext{}, cmds: map[string]*ExecCommand{}}
}

// CreateContext inserts a Pending context. The HTTP layer probes the engine
// and then marks it Running — a timer must not do that.
func (c *Commands) CreateContext(clusterID, language string) *ExecContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	ctx := &ExecContext{
		ID:        fmt.Sprintf("ctx-%d", c.nextID),
		ClusterID: clusterID,
		Language:  language,
		Status:    "Pending",
	}
	c.contexts[ctx.ID] = ctx
	return ctx
}

func (c *Commands) GetContext(id string) (*ExecContext, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, ok := c.contexts[id]
	if !ok {
		return nil, false
	}
	cp := *ctx
	return &cp, true
}

func (c *Commands) SetContextStatus(id, status string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, ok := c.contexts[id]
	if !ok {
		return false
	}
	ctx.Status = status
	return true
}

func (c *Commands) DestroyContext(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.contexts[id]; !ok {
		return false
	}
	delete(c.contexts, id)
	for cid, cmd := range c.cmds {
		if cmd.ContextID == id {
			delete(c.cmds, cid)
		}
	}
	return true
}

func (c *Commands) CreateCommand(clusterID, contextID, language, code string) *ExecCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	cmd := &ExecCommand{
		ID:        fmt.Sprintf("cmd-%d", c.nextID),
		ContextID: contextID,
		ClusterID: clusterID,
		Language:  language,
		Code:      code,
		Status:    "Running",
	}
	c.cmds[cmd.ID] = cmd
	return cmd
}

func (c *Commands) GetCommand(id string) (*ExecCommand, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok := c.cmds[id]
	if !ok {
		return nil, false
	}
	cp := *cmd
	return &cp, true
}

func (c *Commands) FinishCommand(id, status, resultType, data, summary, cause string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok := c.cmds[id]
	if !ok {
		return false
	}
	cmd.Status = status
	cmd.ResultType = resultType
	cmd.Data = data
	cmd.Summary = summary
	cmd.Cause = cause
	return true
}

func (c *Commands) CancelCommand(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok := c.cmds[id]
	if !ok {
		return false
	}
	if cmd.Status == "Finished" || cmd.Status == "Error" {
		return true
	}
	cmd.Status = "Cancelled"
	return true
}
