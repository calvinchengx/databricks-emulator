package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Backend types match Databricks create-scope.
const (
	BackendDatabricks    = "DATABRICKS"
	BackendAzureKeyVault = "AZURE_KEYVAULT"
)

// SecretScope holds keys (Databricks-backed) or vault metadata (AKV-backed).
// Values are never returned over REST.
type SecretScope struct {
	Name       string
	Backend    string
	ResourceID string
	DNSName    string
	keys       map[string]string
}

// Secrets is a file-backed secret store. Databricks-backed values persist
// under data/secrets/; AKV-backed scopes store metadata only.
type Secrets struct {
	mu     sync.Mutex
	dir    string
	scopes map[string]*SecretScope
}

type persistedSecrets struct {
	Scopes map[string]persistedScope `json:"scopes"`
}

type persistedScope struct {
	Name       string            `json:"name"`
	Backend    string            `json:"backend"`
	ResourceID string            `json:"resource_id,omitempty"`
	DNSName    string            `json:"dns_name,omitempty"`
	Keys       map[string]string `json:"keys,omitempty"`
}

func openSecrets(dataDir string) (*Secrets, error) {
	dir := filepath.Join(dataDir, "secrets")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Secrets{dir: dir, scopes: map[string]*SecretScope{}}
	path := filepath.Join(dir, "scopes.json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	var p persistedSecrets
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("secrets store: %w", err)
	}
	for k, sc := range p.Scopes {
		keys := sc.Keys
		if keys == nil {
			keys = map[string]string{}
		}
		backend := sc.Backend
		if backend == "" {
			backend = BackendDatabricks
		}
		s.scopes[normScope(k)] = &SecretScope{
			Name:       sc.Name,
			Backend:    backend,
			ResourceID: sc.ResourceID,
			DNSName:    sc.DNSName,
			keys:       keys,
		}
	}
	return s, nil
}

func normScope(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func (s *Secrets) persistLocked() error {
	p := persistedSecrets{Scopes: map[string]persistedScope{}}
	for k, sc := range s.scopes {
		ps := persistedScope{Name: sc.Name, Backend: sc.Backend, ResourceID: sc.ResourceID, DNSName: sc.DNSName}
		if sc.Backend != BackendAzureKeyVault {
			ps.Keys = sc.keys
		}
		p.Scopes[k] = ps
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "scopes.json.tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, "scopes.json"))
}

// CreateScope adds a Databricks-backed scope.
func (s *Secrets) CreateScope(name string) error {
	return s.create(name, BackendDatabricks, "", "")
}

// CreateAKVScope adds a Key Vault-backed scope. Values stay in the vault.
func (s *Secrets) CreateAKVScope(name, resourceID, dnsName string) error {
	return s.create(name, BackendAzureKeyVault, resourceID, dnsName)
}

func (s *Secrets) create(name, backend, resourceID, dnsName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("scope is required")
	}
	key := normScope(name)
	if _, ok := s.scopes[key]; ok {
		return fmt.Errorf("scope exists")
	}
	s.scopes[key] = &SecretScope{
		Name:       name,
		Backend:    backend,
		ResourceID: resourceID,
		DNSName:    dnsName,
		keys:       map[string]string{},
	}
	return s.persistLocked()
}

// GetScope returns a scope by name.
func (s *Secrets) GetScope(name string) (*SecretScope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.scopes[normScope(name)]
	if !ok {
		return nil, false
	}
	cp := *sc
	return &cp, true
}

// DeleteScope removes a scope.
func (s *Secrets) DeleteScope(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := normScope(name)
	if _, ok := s.scopes[key]; !ok {
		return fmt.Errorf("scope not found")
	}
	delete(s.scopes, key)
	return s.persistLocked()
}

// ListScopes returns scopes (names and backends; never values).
func (s *Secrets) ListScopes() []SecretScope {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []SecretScope
	for _, sc := range s.scopes {
		out = append(out, SecretScope{Name: sc.Name, Backend: sc.Backend, ResourceID: sc.ResourceID, DNSName: sc.DNSName})
	}
	return out
}

// Put stores a value on a Databricks-backed scope.
func (s *Secrets) Put(scope, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.scopes[normScope(scope)]
	if !ok {
		return fmt.Errorf("scope not found")
	}
	if sc.Backend == BackendAzureKeyVault {
		return fmt.Errorf("Cannot write secrets to Azure KeyVault-backed scope")
	}
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("key is required")
	}
	sc.keys[key] = value
	return s.persistLocked()
}

// DeleteKey removes a key from a Databricks-backed scope.
func (s *Secrets) DeleteKey(scope, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.scopes[normScope(scope)]
	if !ok {
		return fmt.Errorf("scope not found")
	}
	if sc.Backend == BackendAzureKeyVault {
		return fmt.Errorf("Cannot write secrets to Azure KeyVault-backed scope")
	}
	if _, ok := sc.keys[key]; !ok {
		return fmt.Errorf("secret not found")
	}
	delete(sc.keys, key)
	return s.persistLocked()
}

// ListKeys returns key names on a Databricks-backed scope, never values.
func (s *Secrets) ListKeys(scope string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.scopes[normScope(scope)]
	if !ok {
		return nil, fmt.Errorf("scope not found")
	}
	if sc.Backend == BackendAzureKeyVault {
		return nil, fmt.Errorf("list AKV-backed keys from the vault")
	}
	var out []string
	for k := range sc.keys {
		out = append(out, k)
	}
	return out, nil
}

// Resolve returns a Databricks-backed value for job injection. REST must not
// call this. AKV-backed scopes are resolved by the server against the vault.
func (s *Secrets) Resolve(scope, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.scopes[normScope(scope)]
	if !ok {
		return "", fmt.Errorf("scope not found")
	}
	if sc.Backend == BackendAzureKeyVault {
		return "", fmt.Errorf("AKV-backed secret must be resolved from the vault")
	}
	v, ok := sc.keys[key]
	if !ok {
		return "", fmt.Errorf("secret not found")
	}
	return v, nil
}
