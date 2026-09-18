// Package spark drives an attached statement-executor. No URL and no
// injected Executor means Jobs fail naming the missing engine.
package spark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Request is one task submission.
//
// NO Env AND NO Conf, deliberately. The agent's /statements handler reads
// `session`, `code`, `kind` and its identity fields and NOTHING ELSE: an `env`
// or `spark_conf` key in this body is discarded without comment, which was
// measured rather than inferred (a statement asking for os.environ back after
// sending env answers None, and `sparkConfig` is read only by /environment).
// Carrying them here made the emulator look like it delivered a task's
// environment when the only thing that ever did was the generated code --
// `pythonPreamble` in internal/server/jobs.go. One delivery path, so a test
// cannot pass by asserting on the other one.
type Request struct {
	Session string
	Code    string
	Kind    string
}

// WarehouseSession is the agent session every warehouse statement runs in.
//
// ONE SESSION, BECAUSE A CATALOG IS NOT SESSION STATE. Warehouse statements
// used to run as "sql-"+statement id, a fresh session each time. That was
// harmless only because the agent put every session on one Spark session: the
// id named a namespace nobody looked at. From fabric-emulator v0.33.0
// (its PR #352) the agent gives each session its OWN Spark Connect session,
// and on Sail that session starts with an EMPTY catalog -- measured, and
// measured here as three red e2e suites: `CREATE TABLE events` then
// `INSERT INTO events` answered "table does not exist", and dbt could not find
// `hive_metastore.default`, because each statement was a different catalog.
//
// Databricks puts tables in a metastore SHARED by every warehouse, so sharing
// one session across all of them is the faithful shape, not a workaround --
// a table created by one statement is visible to the next, and to another
// warehouse, exactly as it is on the product.
//
// WHAT THIS DOES NOT REPRODUCE. Session state is shared along with the
// catalog, so a temp view or a `USE` from one connection is visible to the
// next; on Databricks each connection gets its own. Statements are rewritten
// to qualified names before they reach the engine (internal/sqlshim), so
// nothing here depends on the current database. Jobs keep a session per task
// key, so a table a job creates is still not in this catalog -- that gap is
// real and untouched by this constant.
const WarehouseSession = "sql-warehouse"

// Result is what the engine returned.
type Result struct {
	OK     bool
	Stdout string
	Stderr string
	EName  string
	EValue string
}

// Executor runs code.
type Executor interface {
	Run(req Request) (Result, error)
}

// Agent is the family's HTTP statement agent (Sail behind /statements).
type Agent struct {
	URL    string
	Client *http.Client
}

// NewAgent returns an executor, or nil if url is empty.
func NewAgent(url string) *Agent {
	url = strings.TrimRight(url, "/")
	if url == "" {
		return nil
	}
	return &Agent{URL: url, Client: &http.Client{Timeout: 5 * time.Minute}}
}

// Run posts /statements and maps the agent's response.
func (a *Agent) Run(req Request) (Result, error) {
	if a == nil || a.URL == "" {
		return Result{}, fmt.Errorf("no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL to a statement agent")
	}
	body, _ := json.Marshal(map[string]any{
		"session": req.Session,
		"code":    req.Code,
		"kind":    req.Kind,
	})
	resp, err := a.Client.Post(a.URL+"/statements", "application/json", bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("spark agent: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return Result{}, fmt.Errorf("spark agent: status %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		Status string         `json:"status"`
		EName  string         `json:"ename"`
		EValue string         `json:"evalue"`
		Data   map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return Result{}, fmt.Errorf("spark agent: decode: %w", err)
	}
	stdout := ""
	if out.Data != nil {
		if t, ok := out.Data["text/plain"].(string); ok {
			stdout = t
		}
		if j, ok := out.Data["application/json"]; ok {
			if raw, err := json.Marshal(j); err == nil {
				stdout = string(raw)
			}
		}
	}
	ok := out.Status == "ok" || out.Status == ""
	return Result{OK: ok, Stdout: stdout, EName: out.EName, EValue: out.EValue}, nil
}

// Scripted is a test executor that records every request and returns a
// programmed result, or runs a callback. A handler that reports SUCCESS
// without calling Run fails the mutation witness.
type Scripted struct {
	Calls []Request
	Hook  func(Request) (Result, error)
}

// Run records and delegates.
func (s *Scripted) Run(req Request) (Result, error) {
	s.Calls = append(s.Calls, req)
	if s.Hook != nil {
		return s.Hook(req)
	}
	return Result{OK: true, Stdout: "ok"}, nil
}
