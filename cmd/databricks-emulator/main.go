// Command databricks-emulator runs the Databricks workspace emulator.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/server"
	"github.com/calvinchengx/databricks-emulator/internal/tlscert"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	cfg := config.FromEnvPartial()
	if len(args) > 0 {
		switch args[0] {
		case "version":
			fmt.Println("databricks-emulator", version)
			return nil
		case "healthcheck":
			return healthcheck(cfg.Addr)
		}
	}
	fs := flag.NewFlagSet("databricks-emulator", flag.ContinueOnError)
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "state directory")
	fs.StringVar(&cfg.PublicURL, "public-url", cfg.PublicURL, "advertised origin")
	fs.BoolVar(&cfg.DisableTLS, "disable-tls", cfg.DisableTLS, "serve plain HTTP")
	fs.StringVar(&cfg.SparkAgentURL, "spark-connect-url", cfg.SparkAgentURL, "statement agent URL")
	fs.BoolVar(&cfg.OIDCTLSInsecure, "oidc-tls-insecure", cfg.OIDCTLSInsecure, "skip TLS when fetching federated JWKS")
	fs.StringVar(&cfg.AKVVaultHost, "akv-vault-host", cfg.AKVVaultHost, "keyvault-emulator host:port accepted as a Key Vault (empty = Azure suffixes only)")
	fs.BoolVar(&cfg.AKVTLSInsecure, "akv-tls-insecure", cfg.AKVTLSInsecure, "skip TLS when fetching Key Vault secrets")
	fs.StringVar(&cfg.UCURL, "uc-url", cfg.UCURL, "Unity Catalog OSS sidecar origin")
	fs.BoolVar(&cfg.UCTLSInsecure, "uc-tls-insecure", cfg.UCTLSInsecure, "skip TLS when talking to UC OSS")
	fs.StringVar(&cfg.EntraTokenURL, "entra-token-url", cfg.EntraTokenURL, "optional Entra client-credentials URL for a vault-audience token")
	fs.StringVar(&cfg.EntraClientID, "entra-client-id", cfg.EntraClientID, "confidential client id used to mint the vault-audience token")
	fs.StringVar(&cfg.EntraClientSecret, "entra-client-secret", cfg.EntraClientSecret, "confidential client secret used to mint the vault-audience token")
	if err := fs.Parse(args); err != nil {
		return err
	}

	srv, err := server.New(cfg, nil, nil)
	if err != nil {
		return err
	}
	if srv.Store.FreshSeed {
		fmt.Printf("seeded admin PAT written to %s/admin.pat (printed once)\n", cfg.DataDir)
		fmt.Printf("  PAT: %s\n", srv.Store.AdminPAT)
		fmt.Printf("  OIDC client_id=%s client_secret=%s\n", "databricks-emulator-client", srv.Store.OIDCSecret)
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	scheme := "https"
	if cfg.DisableTLS {
		scheme = "http"
	} else {
		cert, err := tlscert.Load(cfg.DataDir)
		if err != nil {
			return err
		}
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})
	}
	fmt.Printf("databricks-emulator listening on %s://%s\n", scheme, ln.Addr())
	return http.Serve(ln, srv.Handler())
}

func healthcheck(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		host = "127.0.0.1"
	}
	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	resp, err := client.Get("https://" + net.JoinHostPort(host, port) + "/health")
	if err != nil {
		if resp, err = client.Get("http://" + net.JoinHostPort(host, port) + "/health"); err != nil {
			return err
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health: status %d", resp.StatusCode)
	}
	return nil
}
