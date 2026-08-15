package store

import "testing"

func TestCleanRelRejectsTraversal(t *testing.T) {
	ok, err := CleanRel("/a/b/c")
	if err != nil || ok != "a/b/c" {
		t.Fatalf("clean = %q %v", ok, err)
	}
	if _, err := CleanRel("../etc/passwd"); err == nil {
		t.Fatal("traversal accepted")
	}
	if _, err := CleanRel("/a/../../x"); err == nil {
		t.Fatal("escape accepted")
	}
	if got, err := CleanRel("/"); err != nil || got != "" {
		t.Fatalf("root = %q %v", got, err)
	}
	wp, err := WorkspacePath("Shared/etl.py")
	if err != nil || wp != "/Shared/etl.py" {
		t.Fatalf("workspace = %q %v", wp, err)
	}
	dp, err := DBFSPath("dbfs:/tmp/x")
	if err != nil || dp != "/tmp/x" {
		t.Fatalf("dbfs = %q %v", dp, err)
	}
}

func TestObjectIDStableAndNonZero(t *testing.T) {
	a := ObjectID("/Users/admin/tf-hello")
	b := ObjectID("/Users/admin/tf-hello")
	c := ObjectID("/Users/admin/other")
	if a == 0 || a != b || a == c {
		t.Fatalf("object id a=%d b=%d c=%d", a, b, c)
	}
}
