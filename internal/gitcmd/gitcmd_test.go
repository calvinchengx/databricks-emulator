package gitcmd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := LookPath(); err != nil {
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

func initBare(t *testing.T) (remoteURL, src string) {
	t.Helper()
	requireGit(t)
	root := t.TempDir()
	src = filepath.Join(root, "src")
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
	git(t, src, "tag", "v1")
	git(t, src, "checkout", "main")
	bare := filepath.Join(root, "remote.git")
	git(t, root, "clone", "--bare", src, bare)
	return fileURL(t, bare), src
}

func TestCloneAndUpdateAreRealGit(t *testing.T) {
	remote, _ := initBare(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dest := filepath.Join(t.TempDir(), "checkout")
	if err := Clone(ctx, remote, dest, "", ""); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dest, "README.md"))
	if err != nil || string(b) != "hello from remote\n" {
		t.Fatalf("cloned bytes = %q %v", b, err)
	}
	sha, branch, err := Head(ctx, dest)
	if err != nil || branch != "main" || sha == "" {
		t.Fatalf("head %s %s %v", sha, branch, err)
	}
	if err := Update(ctx, dest, "other", "", "", ""); err != nil {
		t.Fatal(err)
	}
	ob, err := os.ReadFile(filepath.Join(dest, "other.txt"))
	if err != nil || string(ob) != "other-branch\n" {
		t.Fatalf("other branch = %q %v", ob, err)
	}
	if err := Update(ctx, dest, "", "v1", "", ""); err != nil {
		t.Fatal(err)
	}
	_, branch, err = Head(ctx, dest)
	if err != nil || branch != "" {
		t.Fatalf("detached tag branch=%q %v", branch, err)
	}
}

func TestCloneMissingRemoteFails(t *testing.T) {
	requireGit(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := Clone(ctx, fileURL(t, filepath.Join(t.TempDir(), "no-such.git")), filepath.Join(t.TempDir(), "dest"), "", "")
	if err == nil {
		t.Fatal("missing remote cloned")
	}
	if !strings.Contains(err.Error(), "git clone") {
		t.Fatalf("error = %v", err)
	}
}
