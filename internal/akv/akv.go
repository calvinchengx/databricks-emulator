// Package akv is the emulator's outbound client to an Azure Key Vault data
// plane — azure-keyvault-emulator in the family composition, or a real vault.
// Used only to resolve AZURE_KEYVAULT-backed secret scopes at use time. The
// secret value is never stored here.
package akv

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// APIVersion is the Key Vault data-plane api-version we speak.
const APIVersion = "7.4"

const maxBody = 1 << 20

// Client fetches secrets from a vault.
type Client struct {
	http        *http.Client
	extraHost   string
	extraScheme string
}

// AzureVaultSuffixes are the Key Vault data-plane domains across Azure's clouds.
var AzureVaultSuffixes = []string{
	".vault.azure.net",
	".vault.azure.cn",
	".vault.usgovcloudapi.net",
	".vault.microsoftazure.de",
	".managedhsm.azure.net",
}

// ErrVaultNotAllowed rejects a vault URI that is not a Key Vault or the
// one configured emulator host. Without this check, a create-scope body
// pointing at an arbitrary URL is SSRF.
var ErrVaultNotAllowed = errors.New("dns_name must be an Azure Key Vault (https://<name>.vault.azure.net) or the configured DATABRICKS_AKV_VAULT_HOST")

// New builds a client. insecure skips TLS verification (the emulator's
// self-signed cert); client overrides when non-nil (tests). extraHost is the
// one non-Azure host:port to accept — the family's keyvault-emulator.
func New(insecure bool, client *http.Client, extraHost string) *Client {
	if client == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		if insecure {
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
		client = &http.Client{Transport: tr}
	}
	scheme := "https"
	if i := strings.Index(extraHost, "://"); i >= 0 {
		scheme, extraHost = extraHost[:i], extraHost[i+3:]
	}
	return &Client{http: client, extraHost: extraHost, extraScheme: scheme}
}

// CheckURI returns the parsed URI if it is one we may dial.
func (c *Client) CheckURI(vaultURI string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(vaultURI))
	if err != nil {
		return nil, fmt.Errorf("%w: unparseable", ErrVaultNotAllowed)
	}
	if u.User != nil || u.Host == "" {
		return nil, ErrVaultNotAllowed
	}
	host := strings.ToLower(u.Hostname())
	if c.extraHost != "" && strings.EqualFold(u.Host, c.extraHost) {
		return u, nil
	}
	if !strings.EqualFold(u.Scheme, "https") {
		return nil, ErrVaultNotAllowed
	}
	for _, suffix := range AzureVaultSuffixes {
		if strings.HasSuffix(host, suffix) && len(host) > len(suffix) {
			return u, nil
		}
	}
	return nil, ErrVaultNotAllowed
}

// ResolveSecret GETs {vaultURI}/secrets/{name}?api-version=… and returns the value.
func (c *Client) ResolveSecret(vaultURI, name string) (string, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return "", fmt.Errorf("INVALID_PARAMETER_VALUE: secret name is not alphanumeric")
	}
	base, err := c.CheckURI(vaultURI)
	if err != nil {
		return "", err
	}
	u := *base
	u.Path = strings.TrimSuffix(base.Path, "/") + "/secrets/" + name
	u.RawPath = strings.TrimSuffix(base.EscapedPath(), "/") + "/secrets/" + url.PathEscape(name)
	u.RawQuery = "api-version=" + APIVersion
	raw, err := c.get(u.String())
	if err != nil {
		return "", err
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("vault returned bad JSON: %w", err)
	}
	return out.Value, nil
}

// ListSecrets returns key names currently in the vault (metadata only).
func (c *Client) ListSecrets(vaultURI string) ([]string, error) {
	base, err := c.CheckURI(vaultURI)
	if err != nil {
		return nil, err
	}
	u := *base
	u.Path = strings.TrimSuffix(base.Path, "/") + "/secrets"
	u.RawQuery = "api-version=" + APIVersion
	raw, err := c.get(u.String())
	if err != nil {
		return nil, err
	}
	var out struct {
		Value []struct {
			ID string `json:"id"`
		} `json:"value"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("vault returned bad JSON: %w", err)
	}
	var names []string
	for _, e := range out.Value {
		id := strings.TrimSuffix(e.ID, "/")
		if i := strings.LastIndex(id, "/"); i >= 0 && i+1 < len(id) {
			names = append(names, id[i+1:])
		}
	}
	return names, nil
}

func (c *Client) get(rawURL string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBody {
		return nil, fmt.Errorf("vault returned more than %d bytes", maxBody)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault rejected the reference (status %d): %s", resp.StatusCode, raw)
	}
	return raw, nil
}
