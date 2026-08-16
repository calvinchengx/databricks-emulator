// Package server is the Databricks workspace HTTP surface.
package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/calvinchengx/databricks-emulator/internal/akv"
	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/clock"
	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/entra"
	"github.com/calvinchengx/databricks-emulator/internal/oidc"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
	"github.com/calvinchengx/databricks-emulator/internal/tlsclient"
	"github.com/calvinchengx/databricks-emulator/internal/uc"
)

// Server is the HTTP API.
type Server struct {
	Cfg    *config.Config
	Store  *store.Store
	Auth   *auth.Authenticator
	OIDC   *oidc.Issuer
	Clock  *clock.Clock
	Spark  spark.Executor
	AKV    *akv.Client
	UC     *uc.Client
	Origin string
}

// New opens the store, OIDC issuer and authenticator.
func New(cfg *config.Config, clk *clock.Clock, exec spark.Executor) (*Server, error) {
	if clk == nil {
		clk = clock.New()
	}
	st, err := store.Open(cfg.DataDir, clk.Now())
	if err != nil {
		return nil, err
	}
	origin := cfg.PublicURL
	if origin == "" {
		scheme := "https"
		if cfg.DisableTLS {
			scheme = "http"
		}
		host := cfg.Addr
		if strings.HasPrefix(host, ":") {
			host = "localhost" + host
		}
		origin = scheme + "://" + host
	}
	iss, err := oidc.Load(cfg.DataDir, origin+"/oidc", origin, clk.Now)
	if err != nil {
		return nil, err
	}
	// One place decides how far each sibling hop is trusted. Pinning the
	// sibling's CA beats skipping verification, and a bad CA path fails
	// startup rather than quietly downgrading to no verification at all.
	oidcClient, err := tlsclient.Trust{CAFile: cfg.SiblingCAFile, Insecure: cfg.OIDCTLSInsecure}.Client(0)
	if err != nil {
		return nil, err
	}
	akvClient, err := tlsclient.Trust{CAFile: cfg.SiblingCAFile, Insecure: cfg.AKVTLSInsecure}.Client(0)
	if err != nil {
		return nil, err
	}
	ucClient, err := tlsclient.Trust{CAFile: cfg.SiblingCAFile, Insecure: cfg.UCTLSInsecure}.Client(30 * time.Second)
	if err != nil {
		return nil, err
	}
	entraClient, err := tlsclient.Trust{CAFile: cfg.SiblingCAFile, Insecure: cfg.OIDCTLSInsecure}.Client(15 * time.Second)
	if err != nil {
		return nil, err
	}

	au := auth.New(st.Identity, iss, cfg.OIDCIssuers, clk.Now, oidcClient)
	if exec == nil && cfg.SparkAgentURL != "" {
		exec = spark.NewAgent(cfg.SparkAgentURL)
	}
	vault := akv.New(akvClient, cfg.AKVVaultHost)
	if cfg.EntraTokenURL != "" {
		vault.Token = entra.NewMinter(cfg.EntraTokenURL, cfg.EntraClientID, cfg.EntraClientSecret, entraClient).VaultToken
	}
	return &Server{
		Cfg:    cfg,
		Store:  st,
		Auth:   au,
		OIDC:   iss,
		Clock:  clk,
		Spark:  exec,
		AKV:    vault,
		UC:     uc.New(cfg.UCURL, ucClient),
		Origin: origin,
	}, nil
}

// Request bodies are read fully into memory, so an unbounded one is a denial
// of service. Raw file uploads get a larger ceiling than the JSON control
// plane. Spark Connect is exempt: its gRPC streams are framed, not buffered.
const (
	MaxJSONBody   int64 = 16 << 20  // 16 MiB
	MaxUploadBody int64 = 256 << 20 // 256 MiB
)

// bodyLimit is the ceiling for a request the mux will route.
func bodyLimit(r *http.Request) int64 {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"),
		strings.HasPrefix(r.URL.Path, "/api/2.0/workspace-files/import-file/"):
		return MaxUploadBody
	default:
		return MaxJSONBody
	}
}

// Handler returns the mux. Unmapped /api/* is 501, never 404.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /.well-known/databricks-config", s.hostMetadata)
	mux.HandleFunc("GET /oidc/.well-known/openid-configuration", s.discovery)
	mux.HandleFunc("GET /oidc/.well-known/oauth-authorization-server", s.discovery)
	mux.HandleFunc("GET /oidc/jwks.json", s.jwks)
	mux.HandleFunc("POST /oidc/v1/token", s.token)

	mux.HandleFunc("GET /api/2.0/preview/scim/v2/Me", s.protect(s.me))
	mux.HandleFunc("GET /api/2.0/scim/v2/Me", s.protect(s.me))
	mux.HandleFunc("POST /api/2.0/token/create", s.protect(s.tokenCreate))
	mux.HandleFunc("GET /api/2.0/token/list", s.protect(s.tokenList))
	mux.HandleFunc("POST /api/2.0/token/delete", s.protect(s.tokenDelete))
	mux.HandleFunc("GET /api/2.0/token/get", s.protect(s.tokenGetRefused))

	mux.HandleFunc("POST /api/2.0/workspace/import", s.protect(s.workspaceImport))
	mux.HandleFunc("GET /api/2.0/workspace/export", s.protect(s.workspaceExport))
	mux.HandleFunc("GET /api/2.0/workspace/get-status", s.protect(s.workspaceStatus))
	mux.HandleFunc("GET /api/2.0/workspace/list", s.protect(s.workspaceList))
	mux.HandleFunc("POST /api/2.0/workspace/mkdirs", s.protect(s.workspaceMkdirs))
	mux.HandleFunc("POST /api/2.0/workspace/delete", s.protect(s.workspaceDelete))
	mux.HandleFunc("POST /api/2.0/workspace-files/import-file/", s.protect(s.workspaceFilesImport))
	mux.HandleFunc("PUT /api/2.0/workspace-files/import-file/", s.protect(s.workspaceFilesImport))
	mux.HandleFunc("GET /api/2.0/workspace-files/", s.protect(s.workspaceFilesGet))

	mux.HandleFunc("POST /api/2.0/git-credentials", s.protect(s.gitCredentialsCreate))
	mux.HandleFunc("GET /api/2.0/git-credentials", s.protect(s.gitCredentialsList))
	mux.HandleFunc("GET /api/2.0/git-credentials/{credential_id}", s.protect(s.gitCredentialsGet))
	mux.HandleFunc("PATCH /api/2.0/git-credentials/{credential_id}", s.protect(s.gitCredentialsUpdate))
	mux.HandleFunc("DELETE /api/2.0/git-credentials/{credential_id}", s.protect(s.gitCredentialsDelete))

	mux.HandleFunc("POST /api/2.0/repos", s.protect(s.reposCreate))
	mux.HandleFunc("GET /api/2.0/repos", s.protect(s.reposList))
	mux.HandleFunc("GET /api/2.0/repos/{repo_id}", s.protect(s.reposGet))
	mux.HandleFunc("PATCH /api/2.0/repos/{repo_id}", s.protect(s.reposUpdate))
	mux.HandleFunc("DELETE /api/2.0/repos/{repo_id}", s.protect(s.reposDelete))

	mux.HandleFunc("POST /api/2.0/dbfs/put", s.protect(s.dbfsPut))
	mux.HandleFunc("GET /api/2.0/dbfs/read", s.protect(s.dbfsRead))
	mux.HandleFunc("GET /api/2.0/dbfs/get-status", s.protect(s.dbfsStatus))
	mux.HandleFunc("GET /api/2.0/dbfs/list", s.protect(s.dbfsList))
	mux.HandleFunc("POST /api/2.0/dbfs/mkdirs", s.protect(s.dbfsMkdirs))
	mux.HandleFunc("POST /api/2.0/dbfs/delete", s.protect(s.dbfsDelete))
	mux.HandleFunc("POST /api/2.0/dbfs/move", s.protect(s.dbfsMove))
	mux.HandleFunc("POST /api/2.0/dbfs/create", s.protect(s.dbfsCreate))
	mux.HandleFunc("POST /api/2.0/dbfs/add-block", s.protect(s.dbfsAddBlock))
	mux.HandleFunc("POST /api/2.0/dbfs/close", s.protect(s.dbfsClose))

	mux.HandleFunc("PUT /api/2.0/fs/files/", s.protect(s.fsPut))
	mux.HandleFunc("GET /api/2.0/fs/files/", s.protect(s.fsGet))
	mux.HandleFunc("HEAD /api/2.0/fs/files/", s.protect(s.fsHead))
	mux.HandleFunc("DELETE /api/2.0/fs/files/", s.protect(s.fsDelete))
	mux.HandleFunc("PUT /api/2.0/fs/directories/", s.protect(s.fsMkdir))
	mux.HandleFunc("GET /api/2.0/fs/directories/", s.protect(s.fsListDir))

	for _, ver := range []string{"2.1", "2.2"} {
		p := "/api/" + ver + "/jobs/"
		mux.HandleFunc("POST "+p+"create", s.protect(s.jobsCreate))
		mux.HandleFunc("POST "+p+"delete", s.protect(s.jobsDelete))
		mux.HandleFunc("POST "+p+"reset", s.protect(s.jobsReset))
		mux.HandleFunc("GET "+p+"get", s.protect(s.jobsGet))
		mux.HandleFunc("GET "+p+"list", s.protect(s.jobsList))
		mux.HandleFunc("POST "+p+"run-now", s.protect(s.jobsRunNow))
		mux.HandleFunc("GET "+p+"runs/get", s.protect(s.jobsRunsGet))
		mux.HandleFunc("GET "+p+"runs/list", s.protect(s.jobsRunsList))
		mux.HandleFunc("GET "+p+"runs/get-output", s.protect(s.jobsRunsOutput))
		mux.HandleFunc("POST "+p+"runs/cancel", s.protect(s.jobsRunsCancel))
	}

	mux.HandleFunc("POST /api/2.0/secrets/scopes/create", s.protect(s.secretsCreateScope))
	mux.HandleFunc("POST /api/2.0/secrets/scopes/delete", s.protect(s.secretsDeleteScope))
	mux.HandleFunc("GET /api/2.0/secrets/scopes/list", s.protect(s.secretsListScopes))
	mux.HandleFunc("POST /api/2.0/secrets/put", s.protect(s.secretsPut))
	mux.HandleFunc("GET /api/2.0/secrets/get", s.protect(s.secretsGet))
	mux.HandleFunc("POST /api/2.0/secrets/delete", s.protect(s.secretsDelete))
	mux.HandleFunc("GET /api/2.0/secrets/list", s.protect(s.secretsList))
	mux.HandleFunc("GET /api/2.0/secrets/acls/list", s.protect(s.secretsACLRefused))
	mux.HandleFunc("GET /api/2.0/secrets/acls/get", s.protect(s.secretsACLRefused))
	mux.HandleFunc("POST /api/2.0/secrets/acls/put", s.protect(s.secretsACLRefused))
	mux.HandleFunc("POST /api/2.0/secrets/acls/delete", s.protect(s.secretsACLRefused))

	mux.HandleFunc("/api/2.1/unity-catalog/", s.protect(s.unityCatalog))
	mux.HandleFunc("/api/2.0/unity-catalog/", s.protect(s.unityCatalog))

	mux.HandleFunc("POST /api/2.0/sql/warehouses", s.protect(s.sqlCreateWarehouse))
	mux.HandleFunc("GET /api/2.0/sql/warehouses", s.protect(s.sqlListWarehouses))
	mux.HandleFunc("GET /api/2.0/sql/warehouses/{id}", s.protect(s.sqlGetWarehouse))
	mux.HandleFunc("DELETE /api/2.0/sql/warehouses/{id}", s.protect(s.sqlDeleteWarehouse))
	mux.HandleFunc("POST /api/2.0/sql/warehouses/{id}/start", s.protect(s.sqlStartWarehouse))
	mux.HandleFunc("POST /api/2.0/sql/warehouses/{id}/stop", s.protect(s.sqlStopWarehouse))
	mux.HandleFunc("POST /api/2.0/sql/statements", s.protect(s.sqlExecuteStatement))
	mux.HandleFunc("GET /api/2.0/sql/statements/{id}", s.protect(s.sqlGetStatement))

	mux.HandleFunc("POST /api/2.0/mcp/sql", s.protect(s.mcpSQL))
	mux.HandleFunc("GET /api/2.0/mcp/sql", s.protect(s.mcpSQL))
	mux.HandleFunc("DELETE /api/2.0/mcp/sql", s.protect(s.mcpSQL))
	mux.HandleFunc("/api/2.0/mcp/", s.protect(s.mcpRefused))

	for _, ver := range []string{"2.0", "2.1"} {
		p := "/api/" + ver + "/clusters/"
		mux.HandleFunc("POST "+p+"create", s.protect(s.clustersCreate))
		mux.HandleFunc("GET "+p+"get", s.protect(s.clustersGet))
		mux.HandleFunc("GET "+p+"list", s.protect(s.clustersList))
		mux.HandleFunc("POST "+p+"start", s.protect(s.clustersStart))
		mux.HandleFunc("POST "+p+"delete", s.protect(s.clustersDelete))
		mux.HandleFunc("POST "+p+"permanent-delete", s.protect(s.clustersDelete))
		mux.HandleFunc("GET "+p+"spark-versions", s.protect(s.clustersSparkVersions))
		mux.HandleFunc("GET "+p+"list-node-types", s.protect(s.clustersNodeTypes))
	}

	mux.HandleFunc("POST /api/2.0/policies/clusters/create", s.protect(s.policiesCreate))
	mux.HandleFunc("GET /api/2.0/policies/clusters/get", s.protect(s.policiesGet))
	mux.HandleFunc("GET /api/2.0/policies/clusters/list", s.protect(s.policiesList))
	mux.HandleFunc("POST /api/2.0/policies/clusters/edit", s.protect(s.policiesEdit))
	mux.HandleFunc("POST /api/2.0/policies/clusters/delete", s.protect(s.policiesDelete))
	mux.HandleFunc("GET /api/2.0/policies/clusters/get-compliance", s.protect(s.policiesGetCompliance))
	mux.HandleFunc("GET /api/2.0/policy-families", s.protect(s.policyFamiliesList))

	mux.HandleFunc("POST /api/1.2/contexts/create", s.protect(s.contextsCreate))
	mux.HandleFunc("GET /api/1.2/contexts/status", s.protect(s.contextsStatus))
	mux.HandleFunc("POST /api/1.2/contexts/destroy", s.protect(s.contextsDestroy))
	mux.HandleFunc("POST /api/1.2/commands/execute", s.protect(s.commandsExecute))
	mux.HandleFunc("GET /api/1.2/commands/status", s.protect(s.commandsStatus))
	mux.HandleFunc("POST /api/1.2/commands/cancel", s.protect(s.commandsCancel))

	mux.HandleFunc("/api/2.0/mlflow/", s.protect(s.mlflow))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern != "" {
			// Serve through the mux so {wildcard} PathValue is populated.
			r.Body = http.MaxBytesReader(w, r.Body, bodyLimit(r))
			mux.ServeHTTP(w, r)
			return
		}
		if isSparkConnectRequest(r) {
			s.protect(s.sparkConnect)(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
				"API endpoint "+r.URL.Path+" is not implemented in databricks-emulator")
			return
		}
		http.NotFound(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) hostMetadata(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"oidc_endpoint": s.Origin + "/oidc",
		"workspace_id":  workspaceOrgID,
	})
}

func (s *Server) discovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.OIDC.Discovery(s.Origin+"/oidc/v1/token", s.Origin+"/oidc/jwks.json"))
}

func (s *Server) jwks(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.OIDC.JWKS())
}

func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if r.Form.Get("grant_type") != "client_credentials" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "grant_type must be client_credentials")
		return
	}
	id, secret := r.Form.Get("client_id"), r.Form.Get("client_secret")
	if id == "" || secret == "" {
		if u, p, ok := r.BasicAuth(); ok {
			id, secret = u, p
		}
	}
	client, ok := s.Store.Identity.LookupClient(id, secret)
	if !ok {
		write401(w, s.Origin+"/oidc", "invalid client")
		return
	}
	tok, err := s.OIDC.IssueAccessToken(client.UserName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": tok,
		"token_type":   "Bearer",
		"expires_in":   s.OIDC.ExpiresIn,
	})
}

// workspaceOrgID is the numeric workspace id the official Terraform
// provider parses from x-databricks-org-id. A non-integer value fails
// strconv.ParseInt in databricks_current_user.
const workspaceOrgID = "1"

func (s *Server) protect(next func(http.ResponseWriter, *http.Request, *auth.Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Auth.AuthenticateRequest(r)
		if err != nil {
			write401(w, s.Origin+"/oidc", err.Error())
			return
		}
		w.Header().Set("x-databricks-org-id", workspaceOrgID)
		next(w, r, p)
	}
}

func (s *Server) me(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          p.ID,
		"userName":    p.UserName,
		"displayName": p.UserName,
		"active":      true,
		"emails":      []map[string]any{{"value": p.UserName + "@local", "primary": true}},
		"home":        "/Users/" + p.UserName,
		"repos":       "/Repos/" + p.UserName,
	})
}

func (s *Server) tokenCreate(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		Comment string `json:"comment"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	value, info, err := s.Store.Identity.CreatePAT(p.UserName, body.Comment, s.Clock.Now())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"token_value": value,
		"token_info": map[string]any{
			"token_id":   info.ID,
			"comment":    info.Comment,
			"created_at": info.CreatedAt,
		},
	})
}

func (s *Server) tokenList(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	var infos []map[string]any
	for _, tok := range s.Store.Identity.ListPATs() {
		infos = append(infos, map[string]any{
			"token_id":   tok.ID,
			"comment":    tok.Comment,
			"created_at": tok.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"token_infos": infos})
}

func (s *Server) tokenDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		TokenID string `json:"token_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if !s.Store.Identity.DeletePAT(body.TokenID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "token not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) tokenGetRefused(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeError(w, http.StatusBadRequest, "BAD_REQUEST",
		"token values are only returned at create time")
}

func decodeJSON(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

func query(r *http.Request, key string) string {
	return r.URL.Query().Get(key)
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func decodeB64(s string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawStdEncoding.DecodeString(s)
}

func pathFromURL(u *url.URL, prefix string) string {
	p := strings.TrimPrefix(u.Path, prefix)
	if q, err := url.PathUnescape(p); err == nil {
		return q
	}
	return p
}

// writeBodyErr reports a body that failed to read. Over the ceiling is 413.
func writeBodyErr(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "MAX_REQUEST_SIZE_EXCEEDED",
			fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit))
		return
	}
	writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
