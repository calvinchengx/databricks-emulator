package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) secretsCreateScope(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Scope                string `json:"scope"`
		ScopeBackendType     string `json:"scope_backend_type"`
		BackendAzureKeyVault *struct {
			ResourceID string `json:"resource_id"`
			DNSName    string `json:"dns_name"`
		} `json:"backend_azure_keyvault"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	backend := strings.ToUpper(strings.TrimSpace(body.ScopeBackendType))
	if backend == "" || backend == store.BackendDatabricks {
		if err := s.Store.Secrets.CreateScope(body.Scope); err != nil {
			if strings.Contains(err.Error(), "exists") {
				writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", err.Error())
				return
			}
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	if backend != store.BackendAzureKeyVault {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "scope_backend_type must be DATABRICKS or AZURE_KEYVAULT")
		return
	}
	if body.BackendAzureKeyVault == nil || strings.TrimSpace(body.BackendAzureKeyVault.DNSName) == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "backend_azure_keyvault.dns_name is required")
		return
	}
	if s.AKV == nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"AZURE_KEYVAULT scopes need a vault: set DATABRICKS_AKV_VAULT_HOST or use an Azure dns_name")
		return
	}
	if _, err := s.AKV.CheckURI(body.BackendAzureKeyVault.DNSName); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if err := s.Store.Secrets.CreateAKVScope(body.Scope, body.BackendAzureKeyVault.ResourceID, body.BackendAzureKeyVault.DNSName); err != nil {
		if strings.Contains(err.Error(), "exists") {
			writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) secretsDeleteScope(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Scope string `json:"scope"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.Secrets.DeleteScope(body.Scope); err != nil {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) secretsListScopes(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	var scopes []map[string]any
	for _, sc := range s.Store.Secrets.ListScopes() {
		scopes = append(scopes, map[string]any{"name": sc.Name, "backend_type": sc.Backend})
	}
	writeJSON(w, http.StatusOK, map[string]any{"scopes": scopes})
}

func (s *Server) secretsPut(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Scope       string `json:"scope"`
		Key         string `json:"key"`
		StringValue string `json:"string_value"`
		BytesValue  string `json:"bytes_value"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if sc, ok := s.Store.Secrets.GetScope(body.Scope); ok && sc.Backend == store.BackendAzureKeyVault {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"Cannot write secrets to Azure KeyVault-backed scope")
		return
	}
	value := body.StringValue
	if value == "" && body.BytesValue != "" {
		raw, err := decodeB64(body.BytesValue)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "bytes_value must be base64")
			return
		}
		value = string(raw)
	}
	if err := s.Store.Secrets.Put(body.Scope, body.Key, value); err != nil {
		if strings.Contains(err.Error(), "KeyVault") {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) secretsGet(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeError(w, http.StatusBadRequest, "BAD_REQUEST",
		"secret values are not readable via the REST API; they resolve only into job environment variables")
}

func (s *Server) secretsDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Scope string `json:"scope"`
		Key   string `json:"key"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if sc, ok := s.Store.Secrets.GetScope(body.Scope); ok && sc.Backend == store.BackendAzureKeyVault {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"Cannot write secrets to Azure KeyVault-backed scope")
		return
	}
	if err := s.Store.Secrets.DeleteKey(body.Scope, body.Key); err != nil {
		if strings.Contains(err.Error(), "KeyVault") {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) secretsList(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	scope := query(r, "scope")
	sc, ok := s.Store.Secrets.GetScope(scope)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "scope not found")
		return
	}
	var keys []string
	var err error
	if sc.Backend == store.BackendAzureKeyVault {
		if s.AKV == nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "no vault is configured for this AKV-backed scope")
			return
		}
		keys, err = s.AKV.ListSecrets(sc.DNSName)
	} else {
		keys, err = s.Store.Secrets.ListKeys(scope)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	var secrets []map[string]any
	for _, k := range keys {
		secrets = append(secrets, map[string]any{"key": k})
	}
	writeJSON(w, http.StatusOK, map[string]any{"secrets": secrets})
}

func (s *Server) secretsACLRefused(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
		"secret ACLs are not implemented: grants would not be enforced")
}

func (s *Server) resolveSecretValue(scope, key string) (string, error) {
	sc, ok := s.Store.Secrets.GetScope(scope)
	if !ok {
		return "", fmt.Errorf("scope not found")
	}
	if sc.Backend == store.BackendAzureKeyVault {
		if s.AKV == nil {
			return "", fmt.Errorf("no vault is configured for this AKV-backed scope")
		}
		return s.AKV.ResolveSecret(sc.DNSName, key)
	}
	return s.Store.Secrets.Resolve(scope, key)
}
