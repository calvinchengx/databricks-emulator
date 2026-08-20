// Package config resolves runtime configuration from DATABRICKS_* environment
// variables with flag overrides applied by cmd.
package config

import (
	"os"
	"strings"
)

// AzureDatabricksAppID is the well-known Azure Databricks resource
// application id. Federated Entra tokens must carry this audience.
const AzureDatabricksAppID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

// Config is the resolved emulator configuration.
type Config struct {
	Addr      string
	DataDir   string
	PublicURL string

	// AgentURL is the origin the STATEMENT AGENT uses to reach this emulator,
	// which is not the same thing as the origin advertised to clients. The
	// agent is another container: a URL that resolves for the client on the
	// host (127.0.0.1:18470) resolves inside the agent to the agent itself.
	// Empty means "same as PublicURL", which is what a single-host deployment
	// wants and what every caller before this got.
	AgentURL   string
	DisableTLS bool

	// OIDCIssuers is an optional list of federated issuer URLs (entra or
	// any OIDC). Empty means only PAT and this process's own OIDC work.
	OIDCIssuers []string
	// OIDCTLSInsecure skips TLS verification when fetching federated JWKS
	// (entra-emulator's self-signed cert on a compose network).
	OIDCTLSInsecure bool

	// SparkAgentURL, when set, is a statement-executor the Jobs runner
	// drives (the family's Spark agent / Sail). Empty means run-now fails
	// naming the missing engine — never SUCCESS.
	SparkAgentURL string
	// SparkConnectGRPCURL, when set, is the Spark Connect gRPC origin
	// Databricks Connect is reverse-proxied to (Sail :50051). Distinct
	// from SparkAgentURL: an HTTP /statements agent is not Spark Connect.
	SparkConnectGRPCURL string

	// AKVVaultHost is the one non-Azure host:port accepted as a Key Vault
	// (keyvault-emulator). Empty: only Azure vault suffixes are allowlisted,
	// so an emulator dns_name fails by name.
	AKVVaultHost string
	// AKVTLSInsecure skips TLS verification when dialing the vault
	// (keyvault-emulator's self-signed cert).
	AKVTLSInsecure bool

	// UCURL, when set, is a Unity Catalog OSS sidecar the /unity-catalog
	// REST is reverse-proxied to after PAT/OIDC. Empty means those routes
	// are 501 naming the missing sidecar — never an invented metastore.
	UCURL string
	// UCTLSInsecure skips TLS verification when dialing UC OSS.
	UCTLSInsecure bool

	// DeltaRoot is the engine-visible URI prefix for warehouse CREATE TABLE
	// that has no LOCATION (managed shape). Sail writes Delta there; UC OSS
	// gets an EXTERNAL table at the same path. Default file:///data/delta/managed.
	DeltaRoot string

	// EntraTokenURL, when set, is the client-credentials endpoint used to
	// mint a vault-audience token for AKV-backed secret resolve. Empty
	// means resolve stays unauthenticated (stand-in vault / make run).
	EntraTokenURL     string
	EntraClientID     string
	EntraClientSecret string

	// SiblingCAFile is a PEM bundle, or a directory of .pem/.crt files,
	// holding the self-signed certificates the sibling emulators serve.
	// Setting it verifies those hops for real and overrides every
	// *_TLS_INSECURE above, which are man-in-the-middle risks kept only
	// because sibling containers do not publish their certificates.
	SiblingCAFile string
}

// FromEnvPartial reads the environment without validating.
func FromEnvPartial() *Config {
	issuers := splitCSV(os.Getenv("DATABRICKS_OIDC_ISSUERS"))
	return &Config{
		Addr:                envOr("DATABRICKS_ADDR", ":8447"),
		DataDir:             envOr("DATABRICKS_DATA_DIR", "./data"),
		PublicURL:           os.Getenv("DATABRICKS_PUBLIC_URL"),
		AgentURL:            os.Getenv("DATABRICKS_AGENT_URL"),
		DisableTLS:          truthy(os.Getenv("DATABRICKS_DISABLE_TLS")),
		OIDCIssuers:         issuers,
		OIDCTLSInsecure:     truthy(os.Getenv("DATABRICKS_OIDC_TLS_INSECURE")),
		SparkAgentURL:       os.Getenv("DATABRICKS_SPARK_CONNECT_URL"),
		SparkConnectGRPCURL: os.Getenv("DATABRICKS_SPARK_CONNECT_GRPC_URL"),
		AKVVaultHost:        os.Getenv("DATABRICKS_AKV_VAULT_HOST"),
		AKVTLSInsecure:      truthy(os.Getenv("DATABRICKS_AKV_TLS_INSECURE")),
		UCURL:               os.Getenv("DATABRICKS_UC_URL"),
		UCTLSInsecure:       truthy(os.Getenv("DATABRICKS_UC_TLS_INSECURE")),
		DeltaRoot:           os.Getenv("DATABRICKS_DELTA_ROOT"),
		EntraTokenURL:       os.Getenv("DATABRICKS_ENTRA_TOKEN_URL"),
		EntraClientID:       os.Getenv("DATABRICKS_ENTRA_CLIENT_ID"),
		EntraClientSecret:   os.Getenv("DATABRICKS_ENTRA_CLIENT_SECRET"),
		SiblingCAFile:       os.Getenv("DATABRICKS_SIBLING_CA_FILE"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
