package oidc

import (
	"strings"
	"testing"
)

func TestLoadIssueAndPersist(t *testing.T) {
	dir := t.TempDir()
	iss, err := Load(dir, "http://iss", "http://aud", func() int64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	tok, err := iss.IssueAccessToken("u")
	if err != nil || strings.Count(tok, ".") != 2 {
		t.Fatalf("token %q %v", tok, err)
	}
	iss2, err := Load(dir, "http://iss", "http://aud", nil)
	if err != nil {
		t.Fatal(err)
	}
	if iss2.KeyID != iss.KeyID {
		t.Fatal("kid not persisted")
	}
	d := iss.Discovery("t", "j")
	if d["issuer"] != "http://iss" {
		t.Fatalf("discovery %+v", d)
	}
	if len(iss.JWKS()["keys"].([]map[string]string)) != 1 {
		t.Fatal("jwks")
	}
	if iss.PublicKey() == nil {
		t.Fatal("pub")
	}
}
