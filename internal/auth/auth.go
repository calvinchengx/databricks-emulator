// Package auth authenticates a Bearer as a workspace PAT, an emulator OIDC
// access token, or (when configured) a federated JWT.
package auth

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"

	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/oidc"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

// Principal is the authenticated caller.
type Principal struct {
	UserName string
	Kind     string // pat | oidc | federated
	ID       string
}

// Errors distinguished for 401 bodies.
var (
	ErrNoToken  = errors.New("missing bearer token")
	ErrBadToken = errors.New("invalid token")
)

// Authenticator dispatches PAT vs JWT and validates each door.
type Authenticator struct {
	Identity  *store.Identity
	OIDC      *oidc.Issuer
	Audiences []string
	Now       func() int64

	client  *http.Client
	mu      sync.RWMutex
	issuers []*issuerSet
}

type issuerSet struct {
	issuer  string
	jwksURL string
	keys    map[string]*rsa.PublicKey
}

// New builds an authenticator. federated is a list of issuer URLs.
func New(ident *store.Identity, iss *oidc.Issuer, federated []string, insecure bool, now func() int64, client *http.Client) *Authenticator {
	if client == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		if insecure {
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
		client = &http.Client{Transport: tr}
	}
	a := &Authenticator{
		Identity: ident,
		OIDC:     iss,
		Audiences: []string{
			iss.Audience,
			iss.Issuer,
			config.AzureDatabricksAppID,
		},
		Now:    now,
		client: client,
	}
	for _, issuer := range federated {
		a.issuers = append(a.issuers, &issuerSet{
			issuer:  strings.TrimRight(issuer, "/"),
			jwksURL: jwksURLFor(issuer),
			keys:    map[string]*rsa.PublicKey{},
		})
	}
	return a
}

func jwksURLFor(issuer string) string {
	issuer = strings.TrimRight(issuer, "/")
	if strings.HasSuffix(issuer, "/v2.0") {
		return strings.TrimSuffix(issuer, "/v2.0") + "/discovery/v2.0/keys"
	}
	return issuer + "/jwks.json"
}

// AuthenticateRequest reads Authorization.
func (a *Authenticator) AuthenticateRequest(r *http.Request) (*Principal, error) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return nil, ErrNoToken
	}
	return a.Authenticate(strings.TrimSpace(h[len(prefix):]))
}

// Authenticate dispatches PAT vs JWT.
func (a *Authenticator) Authenticate(token string) (*Principal, error) {
	if token == "" {
		return nil, ErrNoToken
	}
	if strings.HasPrefix(token, store.PATPrefix) {
		tok, ok := a.Identity.LookupPAT(token)
		if !ok {
			return nil, fmt.Errorf("%w: unknown PAT", ErrBadToken)
		}
		return &Principal{UserName: tok.UserName, Kind: "pat", ID: tok.ID}, nil
	}
	return a.validateJWT(token)
}

func (a *Authenticator) validateJWT(token string) (*Principal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: not a compact JWS", ErrBadToken)
	}
	headB, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: header encoding", ErrBadToken)
	}
	var head struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headB, &head); err != nil || head.Alg != "RS256" {
		return nil, fmt.Errorf("%w: unsupported alg", ErrBadToken)
	}
	key, keyIssuer, err := a.key(head.Kid)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadToken, err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("%w: signature encoding", ErrBadToken)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig); err != nil {
		return nil, fmt.Errorf("%w: signature", ErrBadToken)
	}
	payloadB, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: payload encoding", ErrBadToken)
	}
	var claims struct {
		Iss   string          `json:"iss"`
		Aud   json.RawMessage `json:"aud"`
		Exp   int64           `json:"exp"`
		Nbf   int64           `json:"nbf"`
		Sub   string          `json:"sub"`
		OID   string          `json:"oid"`
		Pref  string          `json:"preferred_username"`
		Uname string          `json:"unique_name"`
	}
	if err := json.Unmarshal(payloadB, &claims); err != nil {
		return nil, fmt.Errorf("%w: claims", ErrBadToken)
	}
	if claims.Iss != keyIssuer {
		return nil, fmt.Errorf("%w: issuer %q not trusted", ErrBadToken, claims.Iss)
	}
	if !audMatch(claims.Aud, a.Audiences) {
		return nil, fmt.Errorf("%w: audience not accepted", ErrBadToken)
	}
	now := a.Now()
	const skew = 60
	if claims.Exp != 0 && now > claims.Exp+skew {
		return nil, fmt.Errorf("%w: expired", ErrBadToken)
	}
	if claims.Nbf != 0 && now < claims.Nbf-skew {
		return nil, fmt.Errorf("%w: not yet valid", ErrBadToken)
	}
	name := claims.Pref
	if name == "" {
		name = claims.Uname
	}
	if name == "" {
		name = claims.Sub
	}
	if name == "" {
		name = claims.OID
	}
	if name == "" {
		return nil, fmt.Errorf("%w: no principal claim", ErrBadToken)
	}
	kind := "federated"
	if claims.Iss == a.OIDC.Issuer {
		kind = "oidc"
	}
	id := claims.OID
	if id == "" {
		id = claims.Sub
	}
	return &Principal{UserName: name, Kind: kind, ID: id}, nil
}

func audMatch(raw json.RawMessage, accepted []string) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		for _, a := range accepted {
			if one == a {
				return true
			}
		}
		return false
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, got := range many {
			for _, a := range accepted {
				if got == a {
					return true
				}
			}
		}
	}
	return false
}

func (a *Authenticator) key(kid string) (*rsa.PublicKey, string, error) {
	if a.OIDC != nil && (kid == a.OIDC.KeyID || kid == "") {
		return a.OIDC.PublicKey(), a.OIDC.Issuer, nil
	}
	a.mu.RLock()
	for _, set := range a.issuers {
		if k := set.keys[kid]; k != nil {
			a.mu.RUnlock()
			return k, set.issuer, nil
		}
	}
	a.mu.RUnlock()
	if a.OIDC != nil && kid == a.OIDC.KeyID {
		return a.OIDC.PublicKey(), a.OIDC.Issuer, nil
	}
	var lastErr error
	for _, set := range a.issuers {
		if err := a.refresh(set); err != nil {
			lastErr = err
		}
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, set := range a.issuers {
		if k := set.keys[kid]; k != nil {
			return k, set.issuer, nil
		}
	}
	if lastErr != nil {
		return nil, "", lastErr
	}
	if a.OIDC != nil {
		return a.OIDC.PublicKey(), a.OIDC.Issuer, nil
	}
	return nil, "", fmt.Errorf("no key %q", kid)
}

func (a *Authenticator) refresh(set *issuerSet) error {
	resp, err := a.client.Get(set.jwksURL)
	if err != nil {
		return fmt.Errorf("fetch JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch JWKS: status %d", resp.StatusCode)
	}
	var doc struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return fmt.Errorf("parse JWKS: %w", err)
	}
	fresh := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		if k.Kty != "RSA" {
			continue
		}
		nB, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			continue
		}
		eB, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			continue
		}
		fresh[k.Kid] = &rsa.PublicKey{
			N: new(big.Int).SetBytes(nB),
			E: int(new(big.Int).SetBytes(eB).Int64()),
		}
	}
	if len(fresh) == 0 {
		return errors.New("JWKS contained no RSA keys")
	}
	a.mu.Lock()
	set.keys = fresh
	a.mu.Unlock()
	return nil
}
