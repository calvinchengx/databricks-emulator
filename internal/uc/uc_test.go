package uc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAttached(t *testing.T) {
	if New("", nil).Attached() {
		t.Fatal("empty URL attached")
	}
	if !New("http://uc:8080", nil).Attached() {
		t.Fatal("set URL not attached")
	}
}

func TestProxyForwardsPathAndStripsAuthorization(t *testing.T) {
	var sawAuth, sawPath, sawQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		sawPath = r.URL.Path
		sawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"catalogs":[{"name":"main"}]}`))
	}))
	defer upstream.Close()

	c := New(upstream.URL, upstream.Client())
	req := httptest.NewRequest(http.MethodGet, "/api/2.1/unity-catalog/catalogs?max_results=10", nil)
	req.Header.Set("Authorization", "Bearer leaked")
	rec := httptest.NewRecorder()
	if err := c.Proxy(rec, req); err != nil {
		t.Fatal(err)
	}
	if sawAuth != "" {
		t.Fatalf("forwarded Authorization: %q", sawAuth)
	}
	if sawPath != "/api/2.1/unity-catalog/catalogs" {
		t.Fatalf("path = %q", sawPath)
	}
	if sawQuery != "max_results=10" {
		t.Fatalf("query = %q", sawQuery)
	}
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"main"`) {
		t.Fatalf("body %s", rec.Body.String())
	}
}

func TestProxyUnreachable(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	req := httptest.NewRequest(http.MethodGet, "/api/2.1/unity-catalog/catalogs", nil)
	if err := c.Proxy(httptest.NewRecorder(), req); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestProxyEmptyBase(t *testing.T) {
	c := New("", nil)
	req := httptest.NewRequest(http.MethodGet, "/api/2.1/unity-catalog/catalogs", nil)
	if err := c.Proxy(httptest.NewRecorder(), req); err == nil {
		t.Fatal("empty base proxied")
	}
}

func TestProxyPostsBody(t *testing.T) {
	var got string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"c"}`))
	}))
	defer upstream.Close()
	c := New(upstream.URL, upstream.Client())
	req := httptest.NewRequest(http.MethodPost, "/api/2.1/unity-catalog/catalogs", strings.NewReader(`{"name":"c"}`))
	req.Header.Set("Content-Type", "application/json")
	if err := c.Proxy(httptest.NewRecorder(), req); err != nil {
		t.Fatal(err)
	}
	if got != `{"name":"c"}` {
		t.Fatalf("body = %q", got)
	}
}

func TestJSONCreateAndAlreadyThere(t *testing.T) {
	var method, path, body string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":"ALREADY_EXISTS"}`))
	}))
	defer upstream.Close()
	c := New(upstream.URL, upstream.Client())
	st, raw, err := c.JSON(http.MethodPost, "/api/2.1/unity-catalog/schemas", map[string]string{
		"name": "gold", "catalog_name": "contoso",
	})
	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/api/2.1/unity-catalog/schemas" {
		t.Fatalf("%s %s", method, path)
	}
	if !strings.Contains(body, `"gold"`) {
		t.Fatalf("body %s", body)
	}
	if !AlreadyThere(st, raw) {
		t.Fatalf("status %d %s", st, raw)
	}
	if _, _, err := New("", nil).JSON(http.MethodGet, "/x", nil); err == nil {
		t.Fatal("empty base")
	}
	if _, _, err := c.JSON("GET /nope", "/x", nil); err == nil {
		t.Fatal("bad method")
	}
	if _, _, err := c.JSON(http.MethodGet, "/x", map[string]any{"ch": make(chan int)}); err == nil {
		t.Fatal("marshal")
	}
	down := New("http://127.0.0.1:1", nil)
	if _, _, err := down.JSON(http.MethodGet, "/api/2.1/unity-catalog/catalogs", nil); err == nil {
		t.Fatal("dial")
	}
	if st, _, err := c.JSON(http.MethodGet, "/api/2.1/unity-catalog/schemas", nil); err != nil || st != http.StatusConflict {
		t.Fatalf("nil body %d %v", st, err)
	}
	if !AlreadyThere(http.StatusOK, nil) || !AlreadyThere(http.StatusCreated, nil) {
		t.Fatal("2xx")
	}
	if !AlreadyThere(http.StatusBadRequest, []byte("already")) {
		t.Fatal("already")
	}
	if !AlreadyThere(http.StatusBadRequest, []byte("exists")) {
		t.Fatal("exists only")
	}
	if AlreadyThere(http.StatusBadRequest, []byte("nope")) || AlreadyThere(http.StatusInternalServerError, nil) {
		t.Fatal("not already")
	}
}
