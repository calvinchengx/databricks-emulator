// Package entra mints tokens from an optional federated STS (entra-emulator
// in family compose). The workspace binary make-runs without it.
package entra

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// VaultScope is the client-credentials scope Key Vault accepts.
const VaultScope = "https://vault.azure.net/.default"

const maxBody = 1 << 20

// Minter exchanges client-credentials for an access token.
type Minter struct {
	TokenURL string
	ClientID string
	Secret   string
	HTTP     *http.Client
}

// NewMinter builds a minter. Empty tokenURL means no STS is attached.
func NewMinter(tokenURL, clientID, secret string, insecure bool, client *http.Client) *Minter {
	tokenURL = strings.TrimSpace(tokenURL)
	if client == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		if insecure {
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
		client = &http.Client{Transport: tr, Timeout: 15 * time.Second}
	}
	return &Minter{TokenURL: tokenURL, ClientID: clientID, Secret: secret, HTTP: client}
}

// Attached is true when a token endpoint was configured.
func (m *Minter) Attached() bool {
	return m != nil && m.TokenURL != ""
}

// VaultToken mints a vault-audience token. Used as akv.Client.Token.
func (m *Minter) VaultToken() (string, error) {
	return m.Token(VaultScope)
}

// Token performs a client-credentials grant at the given scope.
func (m *Minter) Token(scope string) (string, error) {
	if !m.Attached() {
		return "", fmt.Errorf("no Entra token URL is configured — set DATABRICKS_ENTRA_TOKEN_URL")
	}
	if m.ClientID == "" || m.Secret == "" {
		return "", fmt.Errorf("DATABRICKS_ENTRA_CLIENT_ID and DATABRICKS_ENTRA_CLIENT_SECRET are required to mint a vault-audience token")
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {m.ClientID},
		"client_secret": {m.Secret},
		"scope":         {scope},
	}
	resp, err := m.HTTP.PostForm(m.TokenURL, form)
	if err != nil {
		return "", fmt.Errorf("entra unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return "", err
	}
	if len(raw) > maxBody {
		return "", fmt.Errorf("entra returned more than %d bytes", maxBody)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("entra rejected client-credentials (status %d): %s", resp.StatusCode, raw)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("entra returned bad JSON: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("entra returned no access_token")
	}
	return out.AccessToken, nil
}
