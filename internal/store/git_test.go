package store

import "testing"

func TestGitCredentialsPersistAndHideNothingInStore(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Git.CreateCredential("gitHub", "alice", "a@local", "work", "s3cret", true)
	if err != nil || c.ID == 0 || c.Token != "s3cret" {
		t.Fatalf("create %+v %v", c, err)
	}
	s2, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Git.GetCredential(c.ID)
	if !ok || got.Token != "s3cret" || got.Username != "alice" || !got.Default {
		t.Fatalf("reload %+v ok=%v", got, ok)
	}
	if _, ok := s2.Git.GetCredential(999); ok {
		t.Fatal("missing credential")
	}
}

func TestGitReposReservePathAndDrop(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Git.ReserveRepo("file:///tmp/x.git", "gitHub", "/Repos/admin/x", 0)
	if err != nil || r.ID == 0 || r.Path != "/Repos/admin/x" {
		t.Fatalf("reserve %+v %v", r, err)
	}
	if _, err := s.Git.ReserveRepo("file:///tmp/x.git", "gitHub", "/Repos/admin/x", 0); err == nil {
		t.Fatal("duplicate path")
	}
	if _, err := s.Git.ReserveRepo("file:///tmp/x.git", "gitHub", "/Repos/admin/../etc", 0); err == nil {
		t.Fatal("traversal path")
	}
	finished, ok := s.Git.FinishRepo(r.ID, "main", "abc")
	if !ok || finished.Branch != "main" || finished.HeadCommitID != "abc" {
		t.Fatalf("finish %+v", finished)
	}
	at, ok := s.Git.RepoAtPath("/Repos/admin/x")
	if !ok || at.ID != r.ID {
		t.Fatalf("at path %+v", at)
	}
	listed := s.Git.ListRepos("/Repos")
	if len(listed) != 1 {
		t.Fatalf("list %d", len(listed))
	}
	if _, ok := s.Git.DropRepo(r.ID); !ok {
		t.Fatal("drop")
	}
	if _, ok := s.Git.GetRepo(r.ID); ok {
		t.Fatal("dropped repo still there")
	}
}

func TestDefaultRepoPath(t *testing.T) {
	if got := DefaultRepoPath("admin", "file:///tmp/foo/bar.git"); got != "/Repos/admin/bar" {
		t.Fatalf("got %s", got)
	}
}
