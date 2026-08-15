package akv

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestCheckURIAllowlist(t *testing.T) {
	c := New(nil, "keyvault-emulator:4997")
	if _, err := c.CheckURI("https://dev.vault.azure.net/"); err != nil {
		t.Fatalf("azure suffix: %v", err)
	}
	if _, err := c.CheckURI("https://keyvault-emulator:4997"); err != nil {
		t.Fatalf("configured host: %v", err)
	}
	if _, err := c.CheckURI("http://keyvault-emulator:4997"); err != nil {
		t.Fatalf("configured host http: %v", err)
	}
	if _, err := c.CheckURI("https://evil.example"); !errors.Is(err, ErrVaultNotAllowed) {
		t.Fatalf("random host: %v", err)
	}
	if _, err := c.CheckURI("https://vault.azure.net"); !errors.Is(err, ErrVaultNotAllowed) {
		t.Fatalf("bare suffix: %v", err)
	}
	if _, err := c.CheckURI("https://user:pass@dev.vault.azure.net/"); !errors.Is(err, ErrVaultNotAllowed) {
		t.Fatalf("userinfo: %v", err)
	}
	if _, err := c.CheckURI("http://dev.vault.azure.net/"); !errors.Is(err, ErrVaultNotAllowed) {
		t.Fatalf("azure over http: %v", err)
	}
}

func TestCheckURINoExtraHostRefusesEmulator(t *testing.T) {
	c := New(nil, "")
	if _, err := c.CheckURI("https://keyvault-emulator:4997"); !errors.Is(err, ErrVaultNotAllowed) {
		t.Fatalf("emulator host without DATABRICKS_AKV_VAULT_HOST: %v", err)
	}
	if _, err := c.CheckURI("https://dev.vault.azure.net"); err != nil {
		t.Fatalf("azure still allowed: %v", err)
	}
}

func TestResolveAndList(t *testing.T) {
	var gotName string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("api-version") != APIVersion {
			t.Errorf("api-version %q", r.URL.Query().Get("api-version"))
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/secrets/") && r.URL.Path != "/secrets/":
			gotName = strings.TrimPrefix(r.URL.Path, "/secrets/")
			_ = json.NewEncoder(w).Encode(map[string]string{"value": "from-vault"})
		case r.URL.Path == "/secrets":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []map[string]string{
					{"id": "https://vault/secrets/pw"},
					{"id": "https://vault/secrets/token/"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	u, _ := url.Parse(ts.URL)
	c := New(ts.Client(), u.Host)
	v, err := c.ResolveSecret(ts.URL, "pw")
	if err != nil || v != "from-vault" || gotName != "pw" {
		t.Fatalf("resolve %q %v name=%q", v, err, gotName)
	}
	names, err := c.ListSecrets(ts.URL)
	if err != nil || len(names) != 2 || names[0] != "pw" || names[1] != "token" {
		t.Fatalf("list %v %v", names, err)
	}
}

func TestResolveRefusesTraversalName(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "certificates") {
			t.Errorf("name escaped the secrets prefix: %s", r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	u, _ := url.Parse(ts.URL)
	c := New(ts.Client(), u.Host)
	if _, err := c.ResolveSecret(ts.URL, "../../certificates/evil"); err == nil || !strings.Contains(err.Error(), "INVALID_PARAMETER_VALUE") {
		t.Fatalf("traversal name: %v", err)
	}
}

func TestResolveSendsVaultAudienceBearer(t *testing.T) {
	var sawAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		if sawAuth != "Bearer vault-aud" {
			http.Error(w, "AKV10000", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"value": "from-vault"})
	}))
	t.Cleanup(ts.Close)
	u, _ := url.Parse(ts.URL)
	c := New(ts.Client(), u.Host)
	c.Token = func() (string, error) { return "vault-aud", nil }
	v, err := c.ResolveSecret(ts.URL, "pw")
	if err != nil || v != "from-vault" || sawAuth != "Bearer vault-aud" {
		t.Fatalf("resolve %q %v auth=%q", v, err, sawAuth)
	}

	c.Token = func() (string, error) { return "", errors.New("sts down") }
	if _, err := c.ResolveSecret(ts.URL, "pw"); err == nil || !strings.Contains(err.Error(), "vault-audience") {
		t.Fatalf("sts error: %v", err)
	}
	c.Token = func() (string, error) { return "", nil }
	if _, err := c.ResolveSecret(ts.URL, "pw"); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty token: %v", err)
	}
}

func TestResolveVaultError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	t.Cleanup(ts.Close)
	u, _ := url.Parse(ts.URL)
	c := New(ts.Client(), u.Host)
	if _, err := c.ResolveSecret(ts.URL, "pw"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("err = %v", err)
	}
}
