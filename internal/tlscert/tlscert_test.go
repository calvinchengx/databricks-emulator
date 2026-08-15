package tlscert

import (
	"path/filepath"
	"testing"
)

func TestLoadEphemeralAndPersisted(t *testing.T) {
	c1, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if len(c1.Certificate) == 0 {
		t.Fatal("ephemeral cert is empty")
	}

	dir := t.TempDir()
	c2, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	c3, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if string(c2.Certificate[0]) != string(c3.Certificate[0]) {
		t.Fatal("persisted cert was regenerated")
	}
	if _, err := Load(filepath.Join(dir, "missing-parent-is-created")); err != nil {
		t.Fatal(err)
	}
}
