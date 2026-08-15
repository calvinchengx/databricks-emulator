package auth

import "testing"

func TestJWKSURLFor(t *testing.T) {
	if got := jwksURLFor("https://login/tid/v2.0"); got != "https://login/tid/discovery/v2.0/keys" {
		t.Fatalf("entra: %s", got)
	}
	if got := jwksURLFor("http://iss/"); got != "http://iss/jwks.json" {
		t.Fatalf("generic: %s", got)
	}
}
