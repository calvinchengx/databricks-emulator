package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/akv"
	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/entra"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func TestSecretsBytesValueAndSparkConf(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{"scope": "kv"}, nil); st != 200 {
		t.Fatalf("scope %d", st)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte("from-bytes"))
	if st := h.json("POST", "/api/2.0/secrets/put", pat, map[string]any{
		"scope": "kv", "key": "pw", "bytes_value": b64,
	}, nil); st != 200 {
		t.Fatalf("put bytes %d", st)
	}
	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), "FILE", "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "s",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "/s.py"},
			"new_cluster": map[string]any{
				"spark_env_vars": map[string]any{"PW": "{{secrets/kv/pw}}"},
				"spark_conf":     map[string]any{"spark.hadoop.fs.s3a.secret": "{{secrets/kv/pw}}"},
			},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.waitRun(int64(run["run_id"].(float64)))
	if len(h.exec.Calls) == 0 || h.exec.Calls[0].Env["PW"] != "from-bytes" {
		t.Fatalf("env %v", h.exec.Calls)
	}
	if h.exec.Calls[0].Conf["spark.hadoop.fs.s3a.secret"] != "from-bytes" {
		t.Fatalf("spark_conf not resolved: %+v", h.exec.Calls[0].Conf)
	}
	if !strings.Contains(h.exec.Calls[0].Code, "from-bytes") {
		t.Fatalf("bytes secret never reached the driver preamble: %s", h.exec.Calls[0].Code)
	}
}

func TestSecretsACLRefused(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	for _, path := range []string{
		"/api/2.0/secrets/acls/list",
		"/api/2.0/secrets/acls/get",
	} {
		var body map[string]any
		st := h.json("GET", path, pat, nil, &body)
		if st != 501 || body["error_code"] != "NOT_IMPLEMENTED" {
			t.Fatalf("%s → %d %+v", path, st, body)
		}
	}
}

func TestAKVScopeCreateRefusedWithoutAllowlist(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var body map[string]any
	st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{
		"scope":              "kv",
		"scope_backend_type": "AZURE_KEYVAULT",
		"backend_azure_keyvault": map[string]any{
			"resource_id": "/subscriptions/x/vaults/dev",
			"dns_name":    "https://keyvault-emulator:4997",
		},
	}, &body)
	if st != 400 || !strings.Contains(str(body["message"]), "DATABRICKS_AKV_VAULT_HOST") {
		t.Fatalf("create without allowlist %d %+v", st, body)
	}
	if st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{"scope": "local"}, nil); st != 200 {
		t.Fatalf("databricks-backed still works %d", st)
	}
}

func TestAKVScopeReadThroughAndRotate(t *testing.T) {
	var value atomic.Value
	value.Store("first")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/secrets/pw":
			_ = json.NewEncoder(w).Encode(map[string]string{"value": value.Load().(string)})
		case r.URL.Path == "/secrets":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []map[string]string{{"id": "https://vault/secrets/pw"}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	u, _ := url.Parse(ts.URL)

	h := newHarness(t)
	h.srv.AKV = akv.New(ts.Client(), u.Host)
	pat := h.srv.Store.AdminPAT

	if st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{
		"scope":              "kv",
		"scope_backend_type": "AZURE_KEYVAULT",
		"backend_azure_keyvault": map[string]any{
			"resource_id": "/subscriptions/x/vaults/dev",
			"dns_name":    ts.URL,
		},
	}, nil); st != 200 {
		t.Fatalf("create %d", st)
	}
	if st := h.json("POST", "/api/2.0/secrets/put", pat, map[string]any{
		"scope": "kv", "key": "pw", "string_value": "nope",
	}, nil); st != 400 {
		t.Fatalf("put on AKV %d", st)
	}
	if st := h.json("POST", "/api/2.0/secrets/delete", pat, map[string]any{
		"scope": "kv", "key": "pw",
	}, nil); st != 400 {
		t.Fatalf("delete on AKV %d", st)
	}
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/secrets/list?scope=kv", pat, nil, &listed); st != 200 {
		t.Fatalf("list %d", st)
	}
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), `"pw"`) {
		t.Fatalf("list missing vault key: %s", raw)
	}

	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), store.ObjectFile, "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "s",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "/s.py"},
			"new_cluster":       map[string]any{"spark_env_vars": map[string]any{"PW": "{{secrets/kv/pw}}"}},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.waitRun(int64(run["run_id"].(float64)))
	if len(h.exec.Calls) == 0 || h.exec.Calls[0].Env["PW"] != "first" {
		t.Fatalf("first resolve %+v", h.exec.Calls)
	}
	if !strings.Contains(h.exec.Calls[0].Code, "first") {
		t.Fatalf("first vault value never reached the driver: %s", h.exec.Calls[0].Code)
	}

	value.Store("rotated")
	h.exec.Calls = nil
	var run2 map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run2)
	h.waitRun(int64(run2["run_id"].(float64)))
	if len(h.exec.Calls) == 0 || h.exec.Calls[0].Env["PW"] != "rotated" {
		t.Fatalf("rotate did not read through: %+v", h.exec.Calls)
	}
	if !strings.Contains(h.exec.Calls[0].Code, "rotated") {
		t.Fatalf("rotated vault value never reached the driver: %s", h.exec.Calls[0].Code)
	}

	var scopes map[string]any
	h.json("GET", "/api/2.0/secrets/scopes/list", pat, nil, &scopes)
	blob, _ := json.Marshal(scopes)
	if !strings.Contains(string(blob), "AZURE_KEYVAULT") {
		t.Fatalf("list-scopes missing backend: %s", blob)
	}
}

func TestAKVScopeUsesVaultAudienceToken(t *testing.T) {
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("scope") != "https://vault.azure.net/.default" {
			http.Error(w, "wrong scope", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "vault-aud"})
	}))
	t.Cleanup(sts.Close)
	var sawAuth string
	vault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		if sawAuth != "Bearer vault-aud" {
			http.Error(w, "AKV10000", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"value": "from-vault"})
	}))
	t.Cleanup(vault.Close)
	u, _ := url.Parse(vault.URL)

	h := newHarness(t)
	h.srv.AKV = akv.New(vault.Client(), u.Host)
	h.srv.AKV.Token = entra.NewMinter(sts.URL, "app", "sec", sts.Client()).VaultToken
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{
		"scope":              "kv",
		"scope_backend_type": "AZURE_KEYVAULT",
		"backend_azure_keyvault": map[string]any{
			"resource_id": "/subscriptions/x/vaults/dev",
			"dns_name":    vault.URL,
		},
	}, nil); st != 200 {
		t.Fatalf("create %d", st)
	}
	_ = h.srv.Store.Workspace.Put("/s.py", []byte("print(1)"), store.ObjectFile, "PYTHON")
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "s",
		"tasks": []map[string]any{{
			"task_key":          "t",
			"spark_python_task": map[string]any{"python_file": "/s.py"},
			"new_cluster":       map[string]any{"spark_env_vars": map[string]any{"PW": "{{secrets/kv/pw}}"}},
		}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	h.waitRun(int64(run["run_id"].(float64)))
	if len(h.exec.Calls) == 0 || h.exec.Calls[0].Env["PW"] != "from-vault" {
		t.Fatalf("resolve %+v", h.exec.Calls)
	}
	if !strings.Contains(h.exec.Calls[0].Code, "from-vault") {
		t.Fatalf("vault value never reached the driver: %s", h.exec.Calls[0].Code)
	}
	if sawAuth != "Bearer vault-aud" {
		t.Fatalf("vault saw %q", sawAuth)
	}
}

func TestNewWiresEntraVaultToken(t *testing.T) {
	s, err := New(&config.Config{
		DataDir:           t.TempDir(),
		DisableTLS:        true,
		EntraTokenURL:     "http://entra/token",
		EntraClientID:     "app",
		EntraClientSecret: "sec",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.AKV == nil || s.AKV.Token == nil {
		t.Fatal("entra token URL did not wire a vault-audience minter")
	}
	plain, err := New(&config.Config{DataDir: t.TempDir(), DisableTLS: true}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plain.AKV.Token != nil {
		t.Fatal("make run must not require entra")
	}
}

func TestAKVCreateRejectsHostileDNS(t *testing.T) {
	h := newHarness(t)
	h.srv.AKV = akv.New(nil, "keyvault-emulator:4997")
	pat := h.srv.Store.AdminPAT
	var body map[string]any
	st := h.json("POST", "/api/2.0/secrets/scopes/create", pat, map[string]any{
		"scope":              "evil",
		"scope_backend_type": "AZURE_KEYVAULT",
		"backend_azure_keyvault": map[string]any{
			"dns_name": "https://evil.example",
		},
	}, &body)
	if st != 400 {
		t.Fatalf("hostile dns %d %+v", st, body)
	}
}
