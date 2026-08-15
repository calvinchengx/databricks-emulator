package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/gitcmd"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

const gitOpTimeout = 60 * time.Second

func (s *Server) gitCredentialsCreate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		GitProvider          string `json:"git_provider"`
		GitUsername          string `json:"git_username"`
		GitEmail             string `json:"git_email"`
		Name                 string `json:"name"`
		PersonalAccessToken  string `json:"personal_access_token"`
		IsDefaultForProvider *bool  `json:"is_default_for_provider"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	isDefault := body.IsDefaultForProvider != nil && *body.IsDefaultForProvider
	c, err := s.Store.Git.CreateCredential(body.GitProvider, body.GitUsername, body.GitEmail, body.Name, body.PersonalAccessToken, isDefault)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, credJSON(c))
}

func (s *Server) gitCredentialsList(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	creds := s.Store.Git.ListCredentials()
	out := make([]map[string]any, 0, len(creds))
	for _, c := range creds {
		out = append(out, credJSON(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"credentials": out})
}

func (s *Server) gitCredentialsGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(r.PathValue("credential_id"))
	c, ok := s.Store.Git.GetCredential(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "git credential not found")
		return
	}
	writeJSON(w, http.StatusOK, credJSON(c))
}

func (s *Server) gitCredentialsUpdate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(r.PathValue("credential_id"))
	var body struct {
		GitProvider          string `json:"git_provider"`
		GitUsername          string `json:"git_username"`
		GitEmail             string `json:"git_email"`
		Name                 string `json:"name"`
		PersonalAccessToken  string `json:"personal_access_token"`
		IsDefaultForProvider *bool  `json:"is_default_for_provider"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	c, ok := s.Store.Git.UpdateCredential(id, body.GitProvider, body.GitUsername, body.GitEmail, body.Name, body.PersonalAccessToken, body.IsDefaultForProvider)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "git credential not found")
		return
	}
	writeJSON(w, http.StatusOK, credJSON(c))
}

func (s *Server) gitCredentialsDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(r.PathValue("credential_id"))
	if !s.Store.Git.DeleteCredential(id) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "git credential not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func credJSON(c *store.GitCredential) map[string]any {
	out := map[string]any{
		"credential_id": c.ID,
		"git_provider":  c.Provider,
	}
	if c.Username != "" {
		out["git_username"] = c.Username
	}
	if c.Email != "" {
		out["git_email"] = c.Email
	}
	if c.Name != "" {
		out["name"] = c.Name
	}
	if c.Default {
		out["is_default_for_provider"] = true
	}
	return out
}

func (s *Server) reposCreate(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		URL             string `json:"url"`
		Provider        string `json:"provider"`
		Path            string `json:"path"`
		GitCredentialID int64  `json:"git_credential_id"`
		SparseCheckout  any    `json:"sparse_checkout"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if body.SparseCheckout != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "sparse checkout is not implemented")
		return
	}
	if strings.TrimSpace(body.URL) == "" || strings.TrimSpace(body.Provider) == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "url and provider are required")
		return
	}
	if _, err := gitcmd.LookPath(); err != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
		return
	}
	wp := strings.TrimSpace(body.Path)
	if wp == "" {
		wp = store.DefaultRepoPath(p.UserName, body.URL)
	}
	dest, err := s.Store.Workspace.Abs(wp)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if _, err := os.Stat(dest); err == nil {
		writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", wp)
		return
	}
	user, token := "", ""
	credID := body.GitCredentialID
	if c, ok := s.Store.Git.CredentialFor(body.GitCredentialID, body.Provider); ok {
		user, token = c.Username, c.Token
		credID = c.ID
	} else if body.GitCredentialID != 0 {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "git credential not found")
		return
	}
	repo, err := s.Store.Git.ReserveRepo(body.URL, body.Provider, wp, credID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		s.Store.Git.DropRepo(repo.ID)
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), gitOpTimeout)
	defer cancel()
	if err := gitcmd.Clone(ctx, body.URL, dest, user, token); err != nil {
		s.Store.Git.DropRepo(repo.ID)
		_ = os.RemoveAll(dest)
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	sha, branch, err := gitcmd.Head(ctx, dest)
	if err != nil {
		s.Store.Git.DropRepo(repo.ID)
		_ = os.RemoveAll(dest)
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	finished, ok := s.Store.Git.FinishRepo(repo.ID, branch, sha)
	if !ok {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "repo vanished after clone")
		return
	}
	writeJSON(w, http.StatusOK, repoJSON(finished))
}

func (s *Server) reposList(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	repos := s.Store.Git.ListRepos(query(r, "path_prefix"))
	out := make([]map[string]any, 0, len(repos))
	for _, repo := range repos {
		out = append(out, repoJSON(repo))
	}
	writeJSON(w, http.StatusOK, map[string]any{"repos": out})
}

func (s *Server) reposGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(r.PathValue("repo_id"))
	repo, ok := s.Store.Git.GetRepo(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "repo not found")
		return
	}
	writeJSON(w, http.StatusOK, repoJSON(repo))
}

func (s *Server) reposUpdate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(r.PathValue("repo_id"))
	var body struct {
		Branch                     string `json:"branch"`
		Tag                        string `json:"tag"`
		GitCredentialID            int64  `json:"git_credential_id"`
		SparseCheckout             any    `json:"sparse_checkout"`
		DangerouslyForceDiscardAll bool   `json:"dangerously_force_discard_all"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if body.SparseCheckout != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "sparse checkout is not implemented")
		return
	}
	if body.Branch != "" && body.Tag != "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "set branch or tag, not both")
		return
	}
	repo, ok := s.Store.Git.GetRepo(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "repo not found")
		return
	}
	if _, err := gitcmd.LookPath(); err != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
		return
	}
	dest, err := s.Store.Workspace.Abs(repo.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	user, token := "", ""
	if c, ok := s.Store.Git.CredentialFor(body.GitCredentialID, repo.Provider); ok {
		user, token = c.Username, c.Token
	} else if body.GitCredentialID != 0 {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "git credential not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), gitOpTimeout)
	defer cancel()
	if err := gitcmd.Update(ctx, dest, body.Branch, body.Tag, user, token); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	sha, branch, err := gitcmd.Head(ctx, dest)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	finished, ok := s.Store.Git.FinishRepo(id, branch, sha)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "repo not found")
		return
	}
	writeJSON(w, http.StatusOK, repoJSON(finished))
}

func (s *Server) reposDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(r.PathValue("repo_id"))
	repo, ok := s.Store.Git.DropRepo(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "repo not found")
		return
	}
	_ = s.Store.Workspace.Delete(repo.Path, true)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func repoJSON(r *store.Repo) map[string]any {
	out := map[string]any{
		"id":       r.ID,
		"url":      r.URL,
		"provider": r.Provider,
		"path":     r.Path,
	}
	if r.Branch != "" {
		out["branch"] = r.Branch
	}
	if r.HeadCommitID != "" {
		out["head_commit_id"] = r.HeadCommitID
	}
	return out
}

func (s *Server) overlayRepo(obj store.WorkspaceObject) store.WorkspaceObject {
	if repo, ok := s.Store.Git.RepoAtPath(obj.Path); ok {
		obj.ObjectType = store.ObjectRepo
		obj.ObjectID = repo.ID
	}
	return obj
}
