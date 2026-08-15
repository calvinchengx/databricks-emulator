package store

import "testing"

func TestReadAtRejectsExcessiveLength(t *testing.T) {
	d, err := openDBFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put("/a", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadAt("/a", 0, MaxDBFSRead+1); err == nil {
		t.Fatal("length above the 1MiB dbfs/read cap must be refused before allocate")
	}
	if _, err := d.ReadAt("/a", 0, 0); err == nil {
		t.Fatal("non-positive length must be refused")
	}
	got, err := d.ReadAt("/a", 0, 2)
	if err != nil || string(got) != "hi" {
		t.Fatalf("bounded read: %q %v", got, err)
	}
}
