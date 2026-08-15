package server

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/gitcmd"
)

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := gitcmd.LookPath(); err != nil {
		t.Fatal(err)
	}
}

func fileURL(t *testing.T, dir string) string {
	t.Helper()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	abs = filepath.ToSlash(abs)
	if !strings.HasPrefix(abs, "/") {
		abs = "/" + abs
	}
	return "file://" + abs
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", args[0], err, out)
	}
}

func initBareRemote(t *testing.T) string {
	t.Helper()
	requireGit(t)
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.Mkdir(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "README.md"), []byte("hello from remote\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, src, "init", "-b", "main")
	git(t, src, "config", "user.email", "git@local")
	git(t, src, "config", "user.name", "git")
	git(t, src, "add", ".")
	git(t, src, "commit", "-m", "init")
	git(t, src, "checkout", "-b", "other")
	if err := os.WriteFile(filepath.Join(src, "other.txt"), []byte("other-branch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, src, "add", ".")
	git(t, src, "commit", "-m", "other")
	git(t, src, "checkout", "main")
	bare := filepath.Join(root, "remote.git")
	git(t, root, "clone", "--bare", src, bare)
	return fileURL(t, bare)
}

func TestGitCredentialsCRUDAndTokenNeverReturned(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("GET", "/api/2.0/git-credentials", "", nil, nil); st != 401 {
		t.Fatalf("unauth %d", st)
	}
	var created map[string]any
	if st := h.json("POST", "/api/2.0/git-credentials", pat, map[string]any{
		"git_provider": "gitHub", "git_username": "alice", "personal_access_token": "s3cret",
	}, &created); st != 200 {
		t.Fatalf("create %d %+v", st, created)
	}
	if _, ok := created["personal_access_token"]; ok {
		t.Fatalf("token leaked: %+v", created)
	}
	id := int64(created["credential_id"].(float64))
	if id == 0 {
		t.Fatal("no credential_id")
	}
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/git-credentials", pat, nil, &listed); st != 200 {
		t.Fatalf("list %d", st)
	}
	creds, _ := listed["credentials"].([]any)
	if len(creds) != 1 {
		t.Fatalf("list %+v", listed)
	}
	raw, _ := json.Marshal(creds[0])
	if strings.Contains(string(raw), "s3cret") {
		t.Fatalf("token in list: %s", raw)
	}
	var got map[string]any
	if st := h.json("GET", "/api/2.0/git-credentials/"+itoa(id), pat, nil, &got); st != 200 {
		t.Fatalf("get %d", st)
	}
	if _, ok := got["personal_access_token"]; ok {
		t.Fatalf("token on get: %+v", got)
	}
	if st := h.json("PATCH", "/api/2.0/git-credentials/"+itoa(id), pat, map[string]any{
		"git_provider": "gitHub", "git_username": "bob",
	}, &got); st != 200 || got["git_username"] != "bob" {
		t.Fatalf("patch %d %+v", st, got)
	}
	stored, _ := h.srv.Store.Git.GetCredential(id)
	if stored.Token != "s3cret" {
		t.Fatalf("token overwritten %+v", stored)
	}
	if st := h.json("DELETE", "/api/2.0/git-credentials/"+itoa(id), pat, nil, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
	if st := h.json("GET", "/api/2.0/git-credentials/"+itoa(id), pat, nil, nil); st != 404 {
		t.Fatalf("deleted get %d", st)
	}
}

func TestReposCloneCommitPullAndWorkspaceSeesFiles(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	remote := initBareRemote(t)

	if st := h.json("POST", "/api/2.0/git-credentials", pat, map[string]any{
		"git_provider": "gitHub", "git_username": "alice", "personal_access_token": "unused-for-file",
	}, nil); st != 200 {
		t.Fatalf("cred %d", st)
	}

	var repo map[string]any
	if st := h.json("POST", "/api/2.0/repos", pat, map[string]any{
		"url": remote, "provider": "gitHub", "path": "/Repos/admin/e2e",
	}, &repo); st != 200 {
		t.Fatalf("create repo %d %+v", st, repo)
	}
	id := int64(repo["id"].(float64))
	if repo["branch"] != "main" || repo["head_commit_id"] == "" {
		t.Fatalf("clone meta %+v", repo)
	}

	var status map[string]any
	if st := h.json("GET", "/api/2.0/workspace/get-status?path=/Repos/admin/e2e", pat, nil, &status); st != 200 || status["object_type"] != "REPO" {
		t.Fatalf("status %d %+v", st, status)
	}
	var listing map[string]any
	if st := h.json("GET", "/api/2.0/workspace/list?path=/Repos/admin/e2e", pat, nil, &listing); st != 200 {
		t.Fatalf("list %d", st)
	}
	objs, _ := listing["objects"].([]any)
	var sawReadme, sawGit bool
	for _, o := range objs {
		m := o.(map[string]any)
		if m["path"] == "/Repos/admin/e2e/README.md" {
			sawReadme = true
		}
		if strings.HasSuffix(str(m["path"]), "/.git") {
			sawGit = true
		}
	}
	if !sawReadme || sawGit {
		t.Fatalf("listing %+v", listing)
	}
	var exp map[string]any
	if st := h.json("GET", "/api/2.0/workspace/export?path=/Repos/admin/e2e/README.md", pat, nil, &exp); st != 200 {
		t.Fatalf("export %d", st)
	}

	dest, err := h.srv.Store.Workspace.Abs("/Repos/admin/e2e")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "from-workspace.txt"), []byte("committed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dest, "config", "user.email", "git@local")
	git(t, dest, "config", "user.name", "git")
	git(t, dest, "add", "from-workspace.txt")
	git(t, dest, "commit", "-m", "workspace commit")
	git(t, dest, "push", "origin", "main")

	// New commit on the remote, then PATCH pulls it.
	work := t.TempDir()
	git(t, work, "clone", remote, "second")
	second := filepath.Join(work, "second")
	git(t, second, "config", "user.email", "git@local")
	git(t, second, "config", "user.name", "git")
	if err := os.WriteFile(filepath.Join(second, "pulled.txt"), []byte("from-remote\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, second, "add", ".")
	git(t, second, "commit", "-m", "remote advance")
	git(t, second, "push", "origin", "main")

	var updated map[string]any
	if st := h.json("PATCH", "/api/2.0/repos/"+itoa(id), pat, map[string]any{"branch": "main"}, &updated); st != 200 {
		t.Fatalf("update %d %+v", st, updated)
	}
	if updated["head_commit_id"] == repo["head_commit_id"] {
		t.Fatal("head did not advance")
	}
	b, _, err := h.srv.Store.Workspace.Get("/Repos/admin/e2e/pulled.txt")
	if err != nil || string(b) != "from-remote\n" {
		t.Fatalf("pulled file %q %v", b, err)
	}

	if st := h.json("PATCH", "/api/2.0/repos/"+itoa(id), pat, map[string]any{"branch": "other"}, &updated); st != 200 {
		t.Fatalf("checkout other %d %+v", st, updated)
	}
	b, _, err = h.srv.Store.Workspace.Get("/Repos/admin/e2e/other.txt")
	if err != nil || string(b) != "other-branch\n" {
		t.Fatalf("other file %q %v", b, err)
	}

	if st := h.json("POST", "/api/2.0/repos", pat, map[string]any{
		"url": remote, "provider": "gitHub", "sparse_checkout": map[string]any{"patterns": []string{"*.md"}},
	}, nil); st != 501 {
		t.Fatalf("sparse %d", st)
	}

	if st := h.json("DELETE", "/api/2.0/repos/"+itoa(id), pat, nil, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
	if st := h.json("GET", "/api/2.0/repos/"+itoa(id), pat, nil, nil); st != 404 {
		t.Fatalf("deleted get %d", st)
	}
	if st := h.json("GET", "/api/2.0/workspace/get-status?path=/Repos/admin/e2e", pat, nil, nil); st != 404 {
		t.Fatalf("tree remains %d", st)
	}
}

func TestReposCloneMissingRemoteFailsLoudly(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var body map[string]any
	st := h.json("POST", "/api/2.0/repos", pat, map[string]any{
		"url": fileURL(t, filepath.Join(t.TempDir(), "missing.git")), "provider": "gitHub", "path": "/Repos/admin/miss",
	}, &body)
	if st != 400 {
		t.Fatalf("missing remote %d %+v", st, body)
	}
	if !strings.Contains(str(body["message"]), "git clone") {
		t.Fatalf("message %+v", body)
	}
	if _, err := os.Stat(filepath.Join(h.srv.Cfg.DataDir, "workspace", "Repos", "admin", "miss")); !os.IsNotExist(err) {
		t.Fatalf("partial clone left on disk: %v", err)
	}
}
