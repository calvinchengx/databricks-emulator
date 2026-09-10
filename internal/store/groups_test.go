package store

import "testing"

// The rung this round earned: a group written by one store instance must be
// readable by the NEXT one opened over the same directory. An in-memory map
// passes every other assertion here and fails this one, which is exactly the
// failure the round was opened on.
func TestGroupSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	first, err := openGroups(dir)
	if err != nil {
		t.Fatalf("openGroups: %v", err)
	}
	created, err := first.Create("data-platform", []string{"admin"}, 1700000000)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	second, err := openGroups(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := second.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got.DisplayName != "data-platform" {
		t.Fatalf("displayName after reopen = %q, want data-platform", got.DisplayName)
	}
	if len(second.List()) != 1 {
		t.Fatalf("List after reopen = %d groups, want 1", len(second.List()))
	}
}

func TestDuplicateGroupIsRefused(t *testing.T) {
	g, err := openGroups(t.TempDir())
	if err != nil {
		t.Fatalf("openGroups: %v", err)
	}
	if _, err := g.Create("dupe", nil, 1); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if _, err := g.Create("dupe", nil, 2); err != ErrGroupExists {
		t.Fatalf("second Create err = %v, want ErrGroupExists", err)
	}
}

func TestDeletedGroupIsGoneAfterReopen(t *testing.T) {
	dir := t.TempDir()
	first, _ := openGroups(dir)
	g, err := first.Create("temp", nil, 1)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := first.Delete(g.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	second, _ := openGroups(dir)
	if _, err := second.Get(g.ID); err != ErrGroupNotFound {
		t.Fatalf("Get after delete+reopen = %v, want ErrGroupNotFound", err)
	}
}
