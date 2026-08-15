package entra

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestVaultTokenClientCredentials(t *testing.T) {
	var saw url.Values
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		saw, _ = url.ParseQuery(string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"vault-aud","token_type":"Bearer"}`))
	}))
	t.Cleanup(ts.Close)
	m := NewMinter(ts.URL, "app", "secret", ts.Client())
	tok, err := m.VaultToken()
	if err != nil || tok != "vault-aud" {
		t.Fatalf("token %q %v", tok, err)
	}
	if saw.Get("grant_type") != "client_credentials" || saw.Get("scope") != VaultScope {
		t.Fatalf("form %v", saw)
	}
	if saw.Get("client_id") != "app" || saw.Get("client_secret") != "secret" {
		t.Fatalf("creds %v", saw)
	}
}

func TestMinterFailures(t *testing.T) {
	if tok, err := NewMinter("", "a", "b", nil).VaultToken(); err == nil || tok != "" {
		t.Fatalf("empty url %q %v", tok, err)
	}
	if _, err := NewMinter("http://x", "", "b", nil).VaultToken(); err == nil {
		t.Fatal("missing client")
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	t.Cleanup(ts.Close)
	if _, err := NewMinter(ts.URL, "a", "b", ts.Client()).VaultToken(); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("rejected: %v", err)
	}
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"token_type":"Bearer"}`))
	}))
	t.Cleanup(ts2.Close)
	if _, err := NewMinter(ts2.URL, "a", "b", ts2.Client()).VaultToken(); err == nil || !strings.Contains(err.Error(), "no access_token") {
		t.Fatalf("empty token: %v", err)
	}
	ts3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{`))
	}))
	t.Cleanup(ts3.Close)
	if _, err := NewMinter(ts3.URL, "a", "b", ts3.Client()).VaultToken(); err == nil {
		t.Fatal("bad json accepted")
	}
	if NewMinter("http://x", "a", "b", nil).HTTP == nil {
		t.Fatal("insecure client")
	}
	if NewMinter("", "", "", nil).Attached() {
		t.Fatal("empty url attached")
	}
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	if _, err := NewMinter(dead.URL, "a", "b", dead.Client()).VaultToken(); err == nil {
		t.Fatal("unreachable accepted")
	}
}
