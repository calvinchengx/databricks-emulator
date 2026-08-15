// Package store is the emulator's durable state: identity, workspace files,
// DBFS, jobs and secrets.
package store

import (
	"fmt"
	"os"
)

// Store is the root handle.
type Store struct {
	DataDir    string
	AdminPAT   string
	OIDCSecret string
	FreshSeed  bool

	Identity  *Identity
	Workspace *Workspace
	DBFS      *DBFS
	Jobs      *Jobs
	Secrets   *Secrets
	SQL       *SQL
	Clusters  *Clusters
	Git       *Git
	Policies  *Policies
}

// Open creates dataDir, seeds identity on first run, and opens file stores.
func Open(dataDir string, now int64) (*Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	adminPAT, clientSecret, ident, fresh, err := ensureSeeded(dataDir, now)
	if err != nil {
		return nil, err
	}
	ws, err := openWorkspace(dataDir)
	if err != nil {
		return nil, err
	}
	dbfs, err := openDBFS(dataDir)
	if err != nil {
		return nil, err
	}
	secrets, err := openSecrets(dataDir)
	if err != nil {
		return nil, err
	}
	git, err := openGit(dataDir)
	if err != nil {
		return nil, err
	}
	policies, err := openPolicies(dataDir)
	if err != nil {
		return nil, err
	}
	return &Store{
		DataDir:    dataDir,
		AdminPAT:   adminPAT,
		OIDCSecret: clientSecret,
		FreshSeed:  fresh,
		Identity:   ident,
		Workspace:  ws,
		DBFS:       dbfs,
		Jobs:       newJobs(),
		Secrets:    secrets,
		SQL:        newSQL(),
		Clusters:   newClusters(),
		Git:        git,
		Policies:   policies,
	}, nil
}
