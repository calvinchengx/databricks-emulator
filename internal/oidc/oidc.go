// Package oidc is the emulator's own authorization server: discovery, JWKS,
// and client-credentials tokens. This is the non-Entra door.
package oidc

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// AzureDatabricksAppID is accepted as aud on federated and own tokens.
const AzureDatabricksAppID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

// Issuer signs and describes this process's OIDC.
type Issuer struct {
	Issuer    string
	Audience  string
	Audiences []string
	Key       *rsa.PrivateKey
	KeyID     string
	Now       func() int64
	ExpiresIn int64
}

// Load creates or reads a persisted RSA key under dataDir/oidc.
func Load(dataDir, issuerURL, audience string, now func() int64) (*Issuer, error) {
	dir := filepath.Join(dataDir, "oidc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(dir, "key.pem")
	kidPath := filepath.Join(dir, "kid")
	var key *rsa.PrivateKey
	if b, err := os.ReadFile(keyPath); err == nil {
		block, _ := pem.Decode(b)
		if block == nil {
			return nil, fmt.Errorf("oidc key.pem is not PEM")
		}
		parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		key = parsed
	} else {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		key = k
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
		if err := os.WriteFile(keyPath, pemBytes, 0o600); err != nil {
			return nil, err
		}
	}
	kid := ""
	if b, err := os.ReadFile(kidPath); err == nil && len(b) > 0 {
		kid = string(bytesTrim(b))
	} else {
		raw := make([]byte, 8)
		if _, err := rand.Read(raw); err != nil {
			return nil, err
		}
		kid = "emu-" + base64.RawURLEncoding.EncodeToString(raw)
		if err := os.WriteFile(kidPath, []byte(kid), 0o644); err != nil {
			return nil, err
		}
	}
	if now == nil {
		now = func() int64 { return time.Now().Unix() }
	}
	return &Issuer{
		Issuer:    issuerURL,
		Audience:  audience,
		Audiences: []string{audience, AzureDatabricksAppID},
		Key:       key,
		KeyID:     kid,
		Now:       now,
		ExpiresIn: 3600,
	}, nil
}

func bytesTrim(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// Discovery is the OpenID Provider Metadata document.
func (iss *Issuer) Discovery(tokenURL, jwksURL string) map[string]any {
	return map[string]any{
		"issuer":                                iss.Issuer,
		"token_endpoint":                        tokenURL,
		"jwks_uri":                              jwksURL,
		"grant_types_supported":                 []string{"client_credentials"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_post", "client_secret_basic"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
	}
}

// JWKS returns the public key set.
func (iss *Issuer) JWKS() map[string]any {
	n := base64.RawURLEncoding.EncodeToString(iss.Key.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(iss.Key.E)).Bytes())
	return map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			"kid": iss.KeyID,
			"n":   n,
			"e":   e,
		}},
	}
}

// IssueAccessToken mints an RS256 access token for userName.
func (iss *Issuer) IssueAccessToken(userName string) (string, error) {
	now := iss.Now()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": iss.KeyID})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iss":                iss.Issuer,
		"aud":                iss.tokenAudiences(),
		"sub":                userName,
		"preferred_username": userName,
		"iat":                now,
		"nbf":                now,
		"exp":                now + iss.ExpiresIn,
	})
	if err != nil {
		return "", err
	}
	h := base64.RawURLEncoding.EncodeToString(header)
	c := base64.RawURLEncoding.EncodeToString(claims)
	signing := h + "." + c
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, iss.Key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (iss *Issuer) tokenAudiences() []string {
	if len(iss.Audiences) > 0 {
		return iss.Audiences
	}
	return []string{iss.Audience, AzureDatabricksAppID}
}

// PublicKey is the RSA public key for local validation.
func (iss *Issuer) PublicKey() *rsa.PublicKey {
	return &iss.Key.PublicKey
}
