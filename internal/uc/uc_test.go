package uc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAttached(t *testing.T) {
	if New("", false, nil).Attached() {
		t.Fatal("empty URL attached")
	}
	if !New("http://uc:8080", false, nil).Attached() {
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

	c := New(upstream.URL, false, upstream.Client())
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
	c := New("http://127.0.0.1:1", false, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/2.1/unity-catalog/catalogs", nil)
	if err := c.Proxy(httptest.NewRecorder(), req); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestProxyEmptyBase(t *testing.T) {
	c := New("", false, nil)
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
	c := New(upstream.URL, false, upstream.Client())
	req := httptest.NewRequest(http.MethodPost, "/api/2.1/unity-catalog/catalogs", strings.NewReader(`{"name":"c"}`))
	req.Header.Set("Content-Type", "application/json")
	if err := c.Proxy(httptest.NewRecorder(), req); err != nil {
		t.Fatal(err)
	}
	if got != `{"name":"c"}` {
		t.Fatalf("body = %q", got)
	}
}
