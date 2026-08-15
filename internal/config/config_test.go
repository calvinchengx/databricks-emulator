package config

import (
	"testing"
)

func TestFromEnvPartialDefaultsAndOverrides(t *testing.T) {
	t.Setenv("DATABRICKS_ADDR", "")
	t.Setenv("DATABRICKS_DATA_DIR", "")
	t.Setenv("DATABRICKS_PUBLIC_URL", "")
	t.Setenv("DATABRICKS_DISABLE_TLS", "")
	t.Setenv("DATABRICKS_OIDC_ISSUERS", "")
	t.Setenv("DATABRICKS_OIDC_TLS_INSECURE", "")
	t.Setenv("DATABRICKS_SPARK_CONNECT_URL", "")
	t.Setenv("DATABRICKS_AKV_VAULT_HOST", "")
	t.Setenv("DATABRICKS_AKV_TLS_INSECURE", "")
	t.Setenv("DATABRICKS_UC_URL", "")
	t.Setenv("DATABRICKS_UC_TLS_INSECURE", "")
	c := FromEnvPartial()
	if c.Addr != ":8447" || c.DataDir != "./data" {
		t.Fatalf("defaults: %+v", c)
	}
	if c.DisableTLS || len(c.OIDCIssuers) != 0 || c.SparkAgentURL != "" || c.AKVVaultHost != "" || c.AKVTLSInsecure || c.UCURL != "" || c.UCTLSInsecure {
		t.Fatalf("empty env leaked values: %+v", c)
	}

	t.Setenv("DATABRICKS_ADDR", ":9")
	t.Setenv("DATABRICKS_DATA_DIR", "/tmp/dbx")
	t.Setenv("DATABRICKS_PUBLIC_URL", "https://localhost:8447")
	t.Setenv("DATABRICKS_DISABLE_TLS", "true")
	t.Setenv("DATABRICKS_OIDC_ISSUERS", " https://a/v2.0 ,https://b ")
	t.Setenv("DATABRICKS_OIDC_TLS_INSECURE", "1")
	t.Setenv("DATABRICKS_SPARK_CONNECT_URL", "http://sail:8080")
	t.Setenv("DATABRICKS_AKV_VAULT_HOST", "keyvault-emulator:4997")
	t.Setenv("DATABRICKS_AKV_TLS_INSECURE", "true")
	t.Setenv("DATABRICKS_UC_URL", "http://uc:8080")
	t.Setenv("DATABRICKS_UC_TLS_INSECURE", "1")
	c = FromEnvPartial()
	if c.Addr != ":9" || c.DataDir != "/tmp/dbx" || c.PublicURL != "https://localhost:8447" {
		t.Fatalf("overrides: %+v", c)
	}
	if !c.DisableTLS || !c.OIDCTLSInsecure || c.SparkAgentURL != "http://sail:8080" {
		t.Fatalf("flags: %+v", c)
	}
	if c.AKVVaultHost != "keyvault-emulator:4997" || !c.AKVTLSInsecure {
		t.Fatalf("akv: %+v", c)
	}
	if c.UCURL != "http://uc:8080" || !c.UCTLSInsecure {
		t.Fatalf("uc: %+v", c)
	}
	if len(c.OIDCIssuers) != 2 || c.OIDCIssuers[0] != "https://a/v2.0" {
		t.Fatalf("issuers: %v", c.OIDCIssuers)
	}
}

func TestTruthyAndSplit(t *testing.T) {
	for _, v := range []string{"yes", "ON", "True"} {
		if !truthy(v) {
			t.Fatalf("truthy(%q) = false", v)
		}
	}
	if truthy("no") || truthy("") {
		t.Fatal("falsey accepted")
	}
	if splitCSV("  ") != nil {
		t.Fatal("blank csv")
	}
}
