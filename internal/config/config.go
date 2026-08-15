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
	Addr       string
	DataDir    string
	PublicURL  string
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
}

// FromEnvPartial reads the environment without validating.
func FromEnvPartial() *Config {
	issuers := splitCSV(os.Getenv("DATABRICKS_OIDC_ISSUERS"))
	return &Config{
		Addr:            envOr("DATABRICKS_ADDR", ":8447"),
		DataDir:         envOr("DATABRICKS_DATA_DIR", "./data"),
		PublicURL:       os.Getenv("DATABRICKS_PUBLIC_URL"),
		DisableTLS:      truthy(os.Getenv("DATABRICKS_DISABLE_TLS")),
		OIDCIssuers:     issuers,
		OIDCTLSInsecure: truthy(os.Getenv("DATABRICKS_OIDC_TLS_INSECURE")),
		SparkAgentURL:   os.Getenv("DATABRICKS_SPARK_CONNECT_URL"),
		AKVVaultHost:    os.Getenv("DATABRICKS_AKV_VAULT_HOST"),
		AKVTLSInsecure:  truthy(os.Getenv("DATABRICKS_AKV_TLS_INSECURE")),
		UCURL:           os.Getenv("DATABRICKS_UC_URL"),
		UCTLSInsecure:   truthy(os.Getenv("DATABRICKS_UC_TLS_INSECURE")),
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
