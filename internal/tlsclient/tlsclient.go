// Package tlsclient builds the outbound HTTP clients this emulator uses to
// reach its siblings — entra-emulator, azure-keyvault-emulator, a Unity
// Catalog OSS sidecar.
//
// Each sibling serves a self-signed certificate it generates on first run,
// so the family's historical answer was to skip verification altogether.
// That makes every sibling hop a man-in-the-middle target. Pin the
// certificate instead: point CAFile at the PEM the sibling persists (a
// self-signed cert is its own CA) and verification becomes real.
//
// This package is the one place the emulator may disable certificate
// checking, so the escape hatch has a single documented home.
package tlsclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Trust says how far one sibling hop is trusted.
type Trust struct {
	// CAFile is a PEM bundle, or a directory of .pem/.crt files, holding the
	// certificates to trust. When set, verification is real and Insecure is
	// ignored: an operator who supplied the CA gets the safe behaviour even
	// if a stale *_TLS_INSECURE is still in the environment.
	CAFile string
	// Insecure disables verification entirely — the family's *_TLS_INSECURE
	// behaviour, kept because sibling containers do not publish their
	// certificates by default. Prefer CAFile wherever the cert can be shared.
	Insecure bool
}

// Config returns the TLS config for this trust. A zero Trust means the
// system roots, which is what a real Azure endpoint needs.
func (t Trust) Config() (*tls.Config, error) {
	if t.CAFile != "" {
		pool, err := poolFrom(t.CAFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
	}
	if t.Insecure {
		// #nosec G402 -- opt-in escape hatch; see the package doc.
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	return nil, nil
}

// Client builds an HTTP client for this trust. A zero timeout means no
// client-side deadline, matching http.Client's own default.
func (t Trust) Client(timeout time.Duration) (*http.Client, error) {
	cfg, err := t.Config()
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = cfg
	return &http.Client{Transport: tr, Timeout: timeout}, nil
}

// Pinned reports whether this trust verifies against a supplied CA.
func (t Trust) Pinned() bool { return t.CAFile != "" }

// poolFrom reads a PEM file, or every .pem/.crt in a directory, into a pool.
// An unreadable or certificate-free path is an error, never a silent
// fallback to skipping verification.
func poolFrom(path string) (*x509.CertPool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("sibling CA %q: %w", path, err)
	}
	files := []string{path}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("sibling CA %q: %w", path, err)
		}
		files = nil
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			switch strings.ToLower(filepath.Ext(e.Name())) {
			case ".pem", ".crt":
				files = append(files, filepath.Join(path, e.Name()))
			}
		}
		if len(files) == 0 {
			return nil, fmt.Errorf("sibling CA %q: no .pem or .crt files", path)
		}
	}
	pool := x509.NewCertPool()
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("sibling CA %q: %w", f, err)
		}
		if !pool.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("sibling CA %q: no certificate found", f)
		}
	}
	return pool, nil
}
