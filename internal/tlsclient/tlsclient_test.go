package tlsclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// siblingServer starts a TLS server holding its OWN freshly generated
// self-signed certificate, and returns the PEM a sibling would persist.
// httptest.NewTLSServer reuses one shared certificate for every server it
// creates, so pinning one would silently accept all of them — the pin has
// to be tested against genuinely distinct certificates.
func siblingServer(t *testing.T, dir, name string) (*httptest.Server, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	ts.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	ts.StartTLS()
	t.Cleanup(ts.Close)

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return ts, path
}

// A pinned CA verifies for real: the sibling's own cert is accepted.
func TestPinnedCAAcceptsTheSibling(t *testing.T) {
	ts, ca := siblingServer(t, t.TempDir(), "cert.pem")

	c, err := Trust{CAFile: ca}.Client(0)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Get(ts.URL)
	if err != nil {
		t.Fatalf("pinned client rejected the sibling it pinned: %v", err)
	}
	resp.Body.Close()
}

// The pin is a real check, not decoration: an unrelated server is refused.
func TestPinnedCARejectsAnImposter(t *testing.T) {
	_, ca := siblingServer(t, t.TempDir(), "pinned.pem")
	imposter, _ := siblingServer(t, t.TempDir(), "imposter.pem")

	c, err := Trust{CAFile: ca}.Client(0)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := c.Get(imposter.URL); err == nil {
		resp.Body.Close()
		t.Fatal("pinned client accepted a certificate it never pinned")
	}
}

// A supplied CA wins over a stale *_TLS_INSECURE: the imposter stays refused.
func TestCAFileOverridesInsecure(t *testing.T) {
	_, ca := siblingServer(t, t.TempDir(), "pinned.pem")
	imposter, _ := siblingServer(t, t.TempDir(), "imposter.pem")

	trust := Trust{CAFile: ca, Insecure: true}
	if !trust.Pinned() {
		t.Fatal("Pinned() should be true when a CA file is set")
	}
	c, err := trust.Client(0)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := c.Get(imposter.URL); err == nil {
		resp.Body.Close()
		t.Fatal("Insecure overrode an explicit CA pin")
	}
}

// A bad CA path fails loudly instead of degrading to no verification.
func TestBadCAPathIsAnError(t *testing.T) {
	if _, err := (Trust{CAFile: filepath.Join(t.TempDir(), "absent.pem")}).Client(0); err == nil {
		t.Fatal("a missing CA file must be an error, not a silent downgrade")
	}
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Trust{CAFile: empty}).Client(0); err == nil {
		t.Fatal("a certificate-free PEM must be an error")
	}
}

// A directory of certs covers several siblings at once.
func TestDirectoryOfCerts(t *testing.T) {
	dir := t.TempDir()
	a, _ := siblingServer(t, dir, "entra.pem")
	b, _ := siblingServer(t, dir, "keyvault.crt")

	c, err := Trust{CAFile: dir}.Client(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, ts := range []*httptest.Server{a, b} {
		resp, err := c.Get(ts.URL)
		if err != nil {
			t.Fatalf("directory pin rejected a listed sibling: %v", err)
		}
		resp.Body.Close()
	}
}

// A zero Trust is the system roots — what a real Azure endpoint needs.
func TestZeroTrustUsesSystemRoots(t *testing.T) {
	cfg, err := (Trust{}).Config()
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatalf("zero Trust should leave TLS config nil, got %+v", cfg)
	}
}
