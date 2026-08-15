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

func TestReadPEMPinsPersistedCertAndRefusesMissing(t *testing.T) {
	if _, err := ReadPEM(""); err == nil {
		t.Fatal("empty data dir must not invent a cert")
	}
	if _, err := ReadPEM(t.TempDir()); err == nil {
		t.Fatal("missing cert.pem must not generate one")
	}
	dir := t.TempDir()
	if _, err := Load(dir); err != nil {
		t.Fatal(err)
	}
	pem, err := ReadPEM(dir)
	if err != nil || !looksLikeCertPEM(pem) {
		t.Fatalf("ReadPEM after Load: %q %v", pem, err)
	}
}

func looksLikeCertPEM(b []byte) bool {
	return len(b) > 0 && string(b[:10]) == "-----BEGIN"
}
