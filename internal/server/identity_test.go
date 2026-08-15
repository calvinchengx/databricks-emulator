package server

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/clock"
	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/oidc"
)

func TestHealthAnd501(t *testing.T) {
	h := newHarness(t)
	if st := h.json("GET", "/health", "", nil, nil); st != 200 {
		t.Fatalf("health %d", st)
	}
	var errBody map[string]any
	st := h.json("GET", "/api/2.0/instance-pools/list", h.srv.Store.AdminPAT, nil, &errBody)
	if st != 501 || errBody["error_code"] != "NOT_IMPLEMENTED" {
		t.Fatalf("unmapped: %d %+v", st, errBody)
	}
	resp := h.do("GET", "/nope", "", nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("non-api 404: %d", resp.StatusCode)
	}
}

func TestSeededPATMeAndDevRejected(t *testing.T) {
	h := newHarness(t)
	var me map[string]any
	if st := h.json("GET", "/api/2.0/preview/scim/v2/Me", h.srv.Store.AdminPAT, nil, &me); st != 200 {
		t.Fatalf("me %d", st)
	}
	if me["userName"] != "admin" {
		t.Fatalf("me = %+v", me)
	}
	for _, tok := range []string{"dev", "dapiDEADBEEF", "random"} {
		var body map[string]any
		st := h.json("GET", "/api/2.0/preview/scim/v2/Me", tok, nil, &body)
		if st != 401 {
			t.Fatalf("token %q → %d %+v", tok, st, body)
		}
	}
	resp := h.do("GET", "/api/2.0/preview/scim/v2/Me", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != 401 || !strings.Contains(resp.Header.Get("WWW-Authenticate"), "/oidc") {
		t.Fatalf("challenge: %d %s", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}
}

func TestOIDCClientCredentialsMe(t *testing.T) {
	h := newHarness(t)
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {"databricks-emulator-client"},
		"client_secret": {h.srv.Store.OIDCSecret},
	}
	resp, err := h.client.Post(h.http.URL+"/oidc/v1/token", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("token %d %s", resp.StatusCode, b)
	}
	var tok map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&tok)
	access, _ := tok["access_token"].(string)
	var me map[string]any
	if st := h.json("GET", "/api/2.0/preview/scim/v2/Me", access, nil, &me); st != 200 || me["userName"] != "admin" {
		t.Fatalf("oidc me %d %+v", st, me)
	}

	form.Set("client_secret", "wrong")
	resp2, _ := h.client.Post(h.http.URL+"/oidc/v1/token", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	resp2.Body.Close()
	if resp2.StatusCode != 401 {
		t.Fatalf("bad secret %d", resp2.StatusCode)
	}
	form.Set("grant_type", "password")
	form.Set("client_secret", h.srv.Store.OIDCSecret)
	resp3, _ := h.client.Post(h.http.URL+"/oidc/v1/token", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	resp3.Body.Close()
	if resp3.StatusCode != 400 {
		t.Fatalf("bad grant %d", resp3.StatusCode)
	}

	var disc map[string]any
	if st := h.json("GET", "/oidc/.well-known/openid-configuration", "", nil, &disc); st != 200 || disc["issuer"] == nil {
		t.Fatalf("discovery %d %+v", st, disc)
	}
	var jwks map[string]any
	if st := h.json("GET", "/oidc/jwks.json", "", nil, &jwks); st != 200 {
		t.Fatalf("jwks %d", st)
	}
}

func TestPATMintListDeleteAndGetRefused(t *testing.T) {
	h := newHarness(t)
	var created map[string]any
	if st := h.json("POST", "/api/2.0/token/create", h.srv.Store.AdminPAT, map[string]any{"comment": "ci"}, &created); st != 200 {
		t.Fatalf("create %d", st)
	}
	value, _ := created["token_value"].(string)
	if !strings.HasPrefix(value, "dapi") {
		t.Fatalf("value %s", value)
	}
	var me map[string]any
	if st := h.json("GET", "/api/2.0/preview/scim/v2/Me", value, nil, &me); st != 200 {
		t.Fatalf("new pat me %d", st)
	}
	var listed map[string]any
	h.json("GET", "/api/2.0/token/list", h.srv.Store.AdminPAT, nil, &listed)
	info := created["token_info"].(map[string]any)
	id, _ := info["token_id"].(string)
	if st := h.json("GET", "/api/2.0/token/get?token_id="+id, h.srv.Store.AdminPAT, nil, nil); st != 400 {
		t.Fatalf("get value should be 400, got %d", st)
	}
	if st := h.json("POST", "/api/2.0/token/delete", h.srv.Store.AdminPAT, map[string]any{"token_id": id}, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
	if st := h.json("GET", "/api/2.0/preview/scim/v2/Me", value, nil, nil); st != 401 {
		t.Fatalf("revoked pat still works")
	}
	if st := h.json("POST", "/api/2.0/token/delete", h.srv.Store.AdminPAT, map[string]any{"token_id": "nope"}, nil); st != 404 {
		t.Fatalf("delete missing %d", st)
	}
}

func TestFederatedJWTDoors(t *testing.T) {
	foreign, jwksURL := serveForeignOIDC(t)
	tok, err := foreign.IssueAccessToken("alice")
	if err != nil {
		t.Fatal(err)
	}

	// Not configured → 401
	h := newHarness(t)
	if st := h.json("GET", "/api/2.0/preview/scim/v2/Me", tok, nil, nil); st != 401 {
		t.Fatalf("unconfigured federated %d", st)
	}

	cfg := &config.Config{
		DataDir:     t.TempDir(),
		DisableTLS:  true,
		PublicURL:   "http://dbx.test",
		OIDCIssuers: []string{foreign.Issuer},
	}
	s, err := New(cfg, clock.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Point the derived JWKS at the test server. jwksURLFor(issuer) is issuer/jwks.json
	// and serveForeignOIDC uses that path.
	_ = jwksURL
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	req, _ := http.NewRequest("GET", ts.URL+"/api/2.0/preview/scim/v2/Me", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("configured federated %d %s", resp.StatusCode, b)
	}

	// Wrong audience
	foreign.Audiences = []string{"https://not-databricks"}
	badAud, _ := foreign.IssueAccessToken("alice")
	req2, _ := http.NewRequest("GET", ts.URL+"/api/2.0/preview/scim/v2/Me", nil)
	req2.Header.Set("Authorization", "Bearer "+badAud)
	resp2, _ := ts.Client().Do(req2)
	resp2.Body.Close()
	if resp2.StatusCode != 401 {
		t.Fatalf("wrong aud %d", resp2.StatusCode)
	}

	// Expired
	foreign.Audience = "http://dbx.test"
	foreign.ExpiresIn = -120
	expired, _ := foreign.IssueAccessToken("alice")
	req3, _ := http.NewRequest("GET", ts.URL+"/api/2.0/preview/scim/v2/Me", nil)
	req3.Header.Set("Authorization", "Bearer "+expired)
	resp3, _ := ts.Client().Do(req3)
	resp3.Body.Close()
	if resp3.StatusCode != 401 {
		t.Fatalf("expired %d", resp3.StatusCode)
	}

	// Unsigned garbage
	if st := h.json("GET", "/api/2.0/preview/scim/v2/Me", "aaa.bbb.ccc", nil, nil); st != 401 {
		t.Fatalf("unsigned %d", st)
	}
}

func serveForeignOIDC(t *testing.T) (*oidc.Issuer, string) {
	t.Helper()
	var iss *oidc.Issuer
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	var err error
	iss, err = oidc.Load(t.TempDir(), srv.URL, "http://dbx.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(iss.JWKS())
	})
	return iss, srv.URL + "/jwks.json"
}

func TestAuthErrors(t *testing.T) {
	h := newHarness(t)
	if _, err := h.srv.Auth.Authenticate(""); err != auth.ErrNoToken {
		t.Fatalf("empty: %v", err)
	}
	if _, err := h.srv.Auth.AuthenticateRequest(&http.Request{Header: http.Header{}}); err != auth.ErrNoToken {
		t.Fatalf("missing header: %v", err)
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	// sanity that a hand-rolled JWT with a different key fails
	key, _ := rsa.GenerateKey(rand.Reader, 1024)
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "x"})
	claims, _ := json.Marshal(map[string]any{"iss": "nope", "aud": "x", "sub": "u", "exp": 9_999_999_999})
	h := base64.RawURLEncoding.EncodeToString(header)
	c := base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(h + "." + c))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	tok := h + "." + c + "." + base64.RawURLEncoding.EncodeToString(sig)
	hr := newHarness(t)
	if st := hr.json("GET", "/api/2.0/preview/scim/v2/Me", tok, nil, nil); st != 401 {
		t.Fatalf("foreign key %d", st)
	}
}
