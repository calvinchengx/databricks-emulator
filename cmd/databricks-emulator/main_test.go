package main

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/tlscert"
)

func TestRunVersion(t *testing.T) {
	if err := run([]string{"version"}); err != nil {
		t.Fatal(err)
	}
}

func TestHealthcheckBadAddr(t *testing.T) {
	if err := healthcheck(&config.Config{Addr: "not-an-addr", DisableTLS: true}); err == nil {
		t.Fatal("expected error")
	}
}

func TestHealthcheckPinsOwnCertAndRejectsAStranger(t *testing.T) {
	dir := t.TempDir()
	cert, err := tlscert.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	defer srv.Close()

	cfg := &config.Config{Addr: mustHostPort(t, srv.URL), DataDir: dir}
	if err := healthcheck(cfg); err != nil {
		t.Fatalf("pinned cert should verify: %v", err)
	}

	other := t.TempDir()
	if _, err := tlscert.Load(other); err != nil {
		t.Fatal(err)
	}
	if err := healthcheck(&config.Config{Addr: cfg.Addr, DataDir: other}); err == nil {
		t.Fatal("healthcheck must reject a server whose cert is not the pinned one")
	}
}

func TestHealthcheckPlainHTTPAndBadStatus(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	if err := healthcheck(&config.Config{Addr: mustHostPort(t, ok.URL), DisableTLS: true}); err != nil {
		t.Fatalf("http healthcheck: %v", err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	if err := healthcheck(&config.Config{Addr: mustHostPort(t, bad.URL), DisableTLS: true}); err == nil {
		t.Fatal("non-200 must fail")
	}
}

func TestRunBadFlag(t *testing.T) {
	if err := run([]string{"-bogus"}); err == nil {
		t.Fatal("unknown flag accepted")
	}
}

func TestRunUnlistenableAddr(t *testing.T) {
	t.Setenv("DATABRICKS_DATA_DIR", t.TempDir())
	if err := run([]string{"-disable-tls", "-addr", "999.999.999.999:1"}); err == nil {
		t.Fatal("unlistenable addr accepted")
	}
}

func TestPlainHTTPServerAcceptsH2CAndHTTP1(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := newHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Proto))
	}), true)
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	base := "http://" + ln.Addr().String()

	tr := &http.Transport{}
	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	tr.Protocols = p
	h2, err := (&http.Client{Transport: tr}).Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(h2.Body)
	h2.Body.Close()
	if h2.StatusCode != 200 || string(raw) != "HTTP/2.0" {
		t.Fatalf("h2c %d %s", h2.StatusCode, raw)
	}

	h1, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(h1.Body)
	h1.Body.Close()
	if h1.StatusCode != 200 || string(raw) != "HTTP/1.1" {
		t.Fatalf("http1 %d %s", h1.StatusCode, raw)
	}
}

func mustHostPort(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}
