package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// GitCredential is a persisted Git provider token. The token is never
// returned over REST after create.
type GitCredential struct {
	ID       int64
	Provider string
	Username string
	Email    string
	Name     string
	Default  bool
	Token    string
}

// Repo is a workspace checkout of a remote. The working tree lives under
// the workspace file store; this record is the id / url / head.
type Repo struct {
	ID           int64
	URL          string
	Provider     string
	Path         string
	Branch       string
	HeadCommitID string
	CredentialID int64
}

type persistedGit struct {
	NextCredentialID int64           `json:"next_credential_id"`
	NextRepoID       int64           `json:"next_repo_id"`
	Credentials      []persistedCred `json:"credentials"`
	Repos            []persistedRepo `json:"repos"`
}

type persistedCred struct {
	ID       int64  `json:"id"`
	Provider string `json:"provider"`
	Username string `json:"username,omitempty"`
	Email    string `json:"email,omitempty"`
	Name     string `json:"name,omitempty"`
	Default  bool   `json:"default,omitempty"`
	Token    string `json:"token,omitempty"`
}

type persistedRepo struct {
	ID           int64  `json:"id"`
	URL          string `json:"url"`
	Provider     string `json:"provider"`
	Path         string `json:"path"`
	Branch       string `json:"branch,omitempty"`
	HeadCommitID string `json:"head_commit_id,omitempty"`
	CredentialID int64  `json:"credential_id,omitempty"`
}

// Git is file-backed git-credentials and repo metadata under data/git/.
type Git struct {
	mu          sync.Mutex
	dir         string
	nextCred    int64
	nextRepo    int64
	credentials map[int64]*GitCredential
	repos       map[int64]*Repo
}

func openGit(dataDir string) (*Git, error) {
	dir := filepath.Join(dataDir, "git")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	g := &Git{
		dir:         dir,
		credentials: map[int64]*GitCredential{},
		repos:       map[int64]*Repo{},
	}
	path := filepath.Join(dir, "state.json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return g, nil
		}
		return nil, err
	}
	var p persistedGit
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("git store: %w", err)
	}
	g.nextCred = p.NextCredentialID
	g.nextRepo = p.NextRepoID
	for _, c := range p.Credentials {
		g.credentials[c.ID] = &GitCredential{
			ID:       c.ID,
			Provider: c.Provider,
			Username: c.Username,
			Email:    c.Email,
			Name:     c.Name,
			Default:  c.Default,
			Token:    c.Token,
		}
	}
	for _, r := range p.Repos {
		g.repos[r.ID] = &Repo{
			ID:           r.ID,
			URL:          r.URL,
			Provider:     r.Provider,
			Path:         r.Path,
			Branch:       r.Branch,
			HeadCommitID: r.HeadCommitID,
			CredentialID: r.CredentialID,
		}
	}
	return g, nil
}

func (g *Git) persistLocked() error {
	p := persistedGit{
		NextCredentialID: g.nextCred,
		NextRepoID:       g.nextRepo,
	}
	for _, c := range g.credentials {
		p.Credentials = append(p.Credentials, persistedCred{
			ID:       c.ID,
			Provider: c.Provider,
			Username: c.Username,
			Email:    c.Email,
			Name:     c.Name,
			Default:  c.Default,
			Token:    c.Token,
		})
	}
	for _, r := range g.repos {
		p.Repos = append(p.Repos, persistedRepo{
			ID:           r.ID,
			URL:          r.URL,
			Provider:     r.Provider,
			Path:         r.Path,
			Branch:       r.Branch,
			HeadCommitID: r.HeadCommitID,
			CredentialID: r.CredentialID,
		})
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(g.dir, "state.json.tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(g.dir, "state.json"))
}

// CreateCredential stores a provider token. The token is persisted; REST
// must not echo it.
func (g *Git) CreateCredential(provider, username, email, name, token string, isDefault bool) (*GitCredential, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return nil, fmt.Errorf("git_provider is required")
	}
	g.nextCred++
	c := &GitCredential{
		ID:       g.nextCred,
		Provider: provider,
		Username: username,
		Email:    email,
		Name:     name,
		Default:  isDefault,
		Token:    token,
	}
	g.credentials[c.ID] = c
	if err := g.persistLocked(); err != nil {
		return nil, err
	}
	return cloneCred(c), nil
}

// GetCredential returns a copy, including the token for clone auth.
func (g *Git) GetCredential(id int64) (*GitCredential, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.credentials[id]
	if !ok {
		return nil, false
	}
	return cloneCred(c), true
}

// ListCredentials returns copies, including tokens (HTTP strips them).
func (g *Git) ListCredentials() []*GitCredential {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*GitCredential, 0, len(g.credentials))
	for _, c := range g.credentials {
		out = append(out, cloneCred(c))
	}
	return out
}

// UpdateCredential patches fields. Empty token leaves the stored token.
func (g *Git) UpdateCredential(id int64, provider, username, email, name, token string, isDefault *bool) (*GitCredential, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.credentials[id]
	if !ok {
		return nil, false
	}
	if provider != "" {
		c.Provider = provider
	}
	if username != "" {
		c.Username = username
	}
	if email != "" {
		c.Email = email
	}
	if name != "" {
		c.Name = name
	}
	if token != "" {
		c.Token = token
	}
	if isDefault != nil {
		c.Default = *isDefault
	}
	if err := g.persistLocked(); err != nil {
		return nil, false
	}
	return cloneCred(c), true
}

// DeleteCredential removes a credential. Repos that named it keep working
// with the last clone; the next update will fail if they still point at it.
func (g *Git) DeleteCredential(id int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.credentials[id]; !ok {
		return false
	}
	delete(g.credentials, id)
	_ = g.persistLocked()
	return true
}

// CredentialFor returns the named credential, else the default for provider,
// else any credential for that provider, else (nil, false).
func (g *Git) CredentialFor(id int64, provider string) (*GitCredential, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if id != 0 {
		c, ok := g.credentials[id]
		if !ok {
			return nil, false
		}
		return cloneCred(c), true
	}
	provider = strings.TrimSpace(provider)
	var fallback *GitCredential
	for _, c := range g.credentials {
		if !strings.EqualFold(c.Provider, provider) {
			continue
		}
		if c.Default {
			return cloneCred(c), true
		}
		if fallback == nil {
			fallback = c
		}
	}
	if fallback == nil {
		return nil, false
	}
	return cloneCred(fallback), true
}

// ReserveRepo assigns an id and path. The working tree is not created here.
func (g *Git) ReserveRepo(remote, provider, workspacePath string, credentialID int64) (*Repo, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	wp, err := WorkspacePath(workspacePath)
	if err != nil {
		return nil, err
	}
	if wp == "/" {
		return nil, fmt.Errorf("repo path is required")
	}
	for _, r := range g.repos {
		if r.Path == wp {
			return nil, fmt.Errorf("repo already exists at %s", wp)
		}
	}
	g.nextRepo++
	repo := &Repo{
		ID:           g.nextRepo,
		URL:          remote,
		Provider:     provider,
		Path:         wp,
		CredentialID: credentialID,
	}
	g.repos[repo.ID] = repo
	if err := g.persistLocked(); err != nil {
		return nil, err
	}
	return cloneRepo(repo), nil
}

// FinishRepo records branch and head after a successful clone or update.
func (g *Git) FinishRepo(id int64, branch, head string) (*Repo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.repos[id]
	if !ok {
		return nil, false
	}
	r.Branch = branch
	r.HeadCommitID = head
	if err := g.persistLocked(); err != nil {
		return nil, false
	}
	return cloneRepo(r), true
}

// DropRepo removes metadata. The caller deletes the working tree.
func (g *Git) DropRepo(id int64) (*Repo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.repos[id]
	if !ok {
		return nil, false
	}
	delete(g.repos, id)
	_ = g.persistLocked()
	return cloneRepo(r), true
}

// DropRepoByPath removes metadata when the workspace path is deleted.
func (g *Git) DropRepoByPath(workspacePath string) {
	wp, err := WorkspacePath(workspacePath)
	if err != nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, r := range g.repos {
		if r.Path == wp {
			delete(g.repos, id)
			_ = g.persistLocked()
			return
		}
	}
}

// GetRepo returns a copy.
func (g *Git) GetRepo(id int64) (*Repo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.repos[id]
	if !ok {
		return nil, false
	}
	return cloneRepo(r), true
}

// ListRepos returns copies, optionally filtered by path prefix.
func (g *Git) ListRepos(pathPrefix string) []*Repo {
	g.mu.Lock()
	defer g.mu.Unlock()
	prefix, _ := WorkspacePath(pathPrefix)
	out := make([]*Repo, 0, len(g.repos))
	for _, r := range g.repos {
		if prefix != "" && prefix != "/" && r.Path != prefix && !strings.HasPrefix(r.Path, prefix+"/") {
			continue
		}
		out = append(out, cloneRepo(r))
	}
	return out
}

// RepoAtPath is the repo whose workspace path is exactly p.
func (g *Git) RepoAtPath(p string) (*Repo, bool) {
	wp, err := WorkspacePath(p)
	if err != nil {
		return nil, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, r := range g.repos {
		if r.Path == wp {
			return cloneRepo(r), true
		}
	}
	return nil, false
}

// DefaultRepoPath is /Repos/{user}/{name} from the remote URL.
func DefaultRepoPath(user, remote string) string {
	u := strings.TrimSpace(remote)
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	name := path.Base(u)
	if name == "" || name == "." || name == "/" {
		name = "repo"
	}
	if user == "" {
		user = "admin"
	}
	return "/Repos/" + user + "/" + name
}

func cloneCred(c *GitCredential) *GitCredential {
	cp := *c
	return &cp
}

func cloneRepo(r *Repo) *Repo {
	cp := *r
	return &cp
}
