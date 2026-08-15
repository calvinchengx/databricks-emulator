package store

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SeededClientID is the confidential OIDC client written on first run.
const SeededClientID = "databricks-emulator-client"

// SeededAdminUser is the local-dev user the admin PAT belongs to.
const SeededAdminUser = "admin"

// PATPrefix is how real Databricks PATs begin. Bearer dispatch uses it.
const PATPrefix = "dapi"

// Token is a stored PAT metadata row. The value itself is never stored.
type Token struct {
	ID        string `json:"id"`
	Comment   string `json:"comment"`
	UserName  string `json:"user_name"`
	CreatedAt int64  `json:"created_at"`
	Hash      string `json:"hash"`
}

// OIDCClient is a confidential client for /oidc/v1/token.
type OIDCClient struct {
	ID         string `json:"id"`
	SecretHash string `json:"secret_hash"`
	UserName   string `json:"user_name"`
}

type identityFile struct {
	Tokens  []Token      `json:"tokens"`
	Clients []OIDCClient `json:"clients"`
}

// Identity holds hashed PATs and OIDC clients.
type Identity struct {
	mu      sync.Mutex
	path    string
	tokens  []Token
	clients []OIDCClient
}

func openIdentity(dataDir string) (*Identity, error) {
	id := &Identity{path: filepath.Join(dataDir, "identity.json")}
	b, err := os.ReadFile(id.path)
	if err != nil {
		if os.IsNotExist(err) {
			return id, nil
		}
		return nil, err
	}
	var f identityFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	id.tokens = f.Tokens
	id.clients = f.Clients
	return id, nil
}

func (id *Identity) persist() error {
	if id.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(id.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(identityFile{Tokens: id.tokens, Clients: id.clients}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(id.path, b, 0o600)
}

// LookupPAT returns the token row for a presented value, or false.
// Comparison is constant-time against every stored hash.
func (id *Identity) LookupPAT(value string) (Token, bool) {
	want := hashSecret(value)
	id.mu.Lock()
	defer id.mu.Unlock()
	var found Token
	ok := false
	for _, tok := range id.tokens {
		if subtle.ConstantTimeCompare([]byte(tok.Hash), []byte(want)) == 1 {
			found = tok
			ok = true
		}
	}
	return found, ok
}

// LookupClient validates a confidential client's secret.
func (id *Identity) LookupClient(clientID, secret string) (OIDCClient, bool) {
	want := hashSecret(secret)
	id.mu.Lock()
	defer id.mu.Unlock()
	var found OIDCClient
	ok := false
	for _, c := range id.clients {
		if c.ID != clientID {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(c.SecretHash), []byte(want)) == 1 {
			found = c
			ok = true
		}
	}
	return found, ok
}

// CreatePAT stores a new hashed PAT and returns the plaintext once.
func (id *Identity) CreatePAT(user, comment string, now int64) (value string, info Token, err error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", Token{}, err
	}
	value = PATPrefix + hex.EncodeToString(raw)
	info = Token{
		ID:        hex.EncodeToString(raw[:8]),
		Comment:   comment,
		UserName:  user,
		CreatedAt: now,
		Hash:      hashSecret(value),
	}
	id.mu.Lock()
	defer id.mu.Unlock()
	id.tokens = append(id.tokens, info)
	return value, info, id.persist()
}

// DeletePAT removes a token by id.
func (id *Identity) DeletePAT(tokenID string) bool {
	id.mu.Lock()
	defer id.mu.Unlock()
	for i, tok := range id.tokens {
		if tok.ID == tokenID {
			id.tokens = append(id.tokens[:i], id.tokens[i+1:]...)
			_ = id.persist()
			return true
		}
	}
	return false
}

// ListPATs returns metadata without hashes.
func (id *Identity) ListPATs() []Token {
	id.mu.Lock()
	defer id.mu.Unlock()
	out := make([]Token, len(id.tokens))
	for i, tok := range id.tokens {
		cp := tok
		cp.Hash = ""
		out[i] = cp
	}
	return out
}

func (id *Identity) addClient(c OIDCClient) error {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.clients = append(id.clients, c)
	return id.persist()
}

func hashSecret(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])
}

func writeSecretFile(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(value+"\n"), 0o600)
}

func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(bytesTrimNL(b)), nil
}

func bytesTrimNL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func ensureSeeded(dataDir string, now int64) (adminPAT, clientSecret string, id *Identity, seeded bool, err error) {
	id, err = openIdentity(dataDir)
	if err != nil {
		return "", "", nil, false, err
	}
	patPath := filepath.Join(dataDir, "admin.pat")
	clientPath := filepath.Join(dataDir, "oidc-client.json")
	if len(id.tokens) > 0 && len(id.clients) > 0 {
		adminPAT, err = readSecretFile(patPath)
		if err != nil {
			return "", "", nil, false, fmt.Errorf("identity exists but admin.pat is unreadable: %w", err)
		}
		var seededClient struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		}
		b, rerr := os.ReadFile(clientPath)
		if rerr != nil {
			return "", "", nil, false, fmt.Errorf("identity exists but oidc-client.json is unreadable: %w", rerr)
		}
		if err := json.Unmarshal(b, &seededClient); err != nil {
			return "", "", nil, false, err
		}
		return adminPAT, seededClient.ClientSecret, id, false, nil
	}

	adminPAT, _, err = id.CreatePAT(SeededAdminUser, "seeded admin PAT", now)
	if err != nil {
		return "", "", nil, false, err
	}
	if err := writeSecretFile(patPath, adminPAT); err != nil {
		return "", "", nil, false, err
	}
	clientSecret, err = randomHex(16)
	if err != nil {
		return "", "", nil, false, err
	}
	if err := id.addClient(OIDCClient{
		ID:         SeededClientID,
		SecretHash: hashSecret(clientSecret),
		UserName:   SeededAdminUser,
	}); err != nil {
		return "", "", nil, false, err
	}
	body, _ := json.MarshalIndent(map[string]string{
		"client_id":     SeededClientID,
		"client_secret": clientSecret,
		"user_name":     SeededAdminUser,
	}, "", "  ")
	if err := os.WriteFile(clientPath, body, 0o600); err != nil {
		return "", "", nil, false, err
	}
	return adminPAT, clientSecret, id, true, nil
}
