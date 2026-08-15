package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/uc"
)

func TestUnityCatalogMissingSidecarIs501(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("GET", "/api/2.1/unity-catalog/catalogs", pat, nil, nil); st != 501 {
		t.Fatalf("no sidecar %d", st)
	}
	if st := h.json("GET", "/api/2.0/unity-catalog/catalogs", pat, nil, nil); st != 501 {
		t.Fatalf("2.0 alias %d", st)
	}
}

func TestUnityCatalogRequiresAuth(t *testing.T) {
	h := newHarness(t)
	if st := h.json("GET", "/api/2.1/unity-catalog/catalogs", "", nil, nil); st != 401 {
		t.Fatalf("unauthenticated %d", st)
	}
}

func TestUnityCatalogProxiesCatalogSchemaTableAndRefusesManagedAndGrants(t *testing.T) {
	var lastMethod, lastPath string
	var lastBody []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastMethod, lastPath = r.Method, r.URL.Path
		lastBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/2.1/unity-catalog/catalogs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"main","id":"cat-1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/2.1/unity-catalog/catalogs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"catalogs":[{"name":"main"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/2.1/unity-catalog/schemas":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"default","catalog_name":"main","full_name":"main.default"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/2.1/unity-catalog/tables":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"t","catalog_name":"main","schema_name":"default","table_type":"EXTERNAL","table_id":"tbl-1","storage_location":"file:///tmp/t"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/2.1/unity-catalog/tables/main.default.t":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"t","table_id":"tbl-1","table_type":"EXTERNAL"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":"NOT_FOUND"}`))
		}
	}))
	defer upstream.Close()

	h := newHarness(t)
	h.srv.UC = uc.New(upstream.URL, false, upstream.Client())
	pat := h.srv.Store.AdminPAT

	var created map[string]any
	if st := h.json("POST", "/api/2.1/unity-catalog/catalogs", pat, map[string]any{
		"name": "main", "comment": "oss",
	}, &created); st != 200 {
		t.Fatalf("create catalog %d", st)
	}
	if created["name"] != "main" || created["id"] != "cat-1" {
		t.Fatalf("create catalog %+v", created)
	}
	if lastMethod != http.MethodPost || lastPath != "/api/2.1/unity-catalog/catalogs" {
		t.Fatalf("upstream create %s %s", lastMethod, lastPath)
	}

	var listed map[string]any
	if st := h.json("GET", "/api/2.1/unity-catalog/catalogs", pat, nil, &listed); st != 200 {
		t.Fatalf("list %d", st)
	}
	cats, _ := listed["catalogs"].([]any)
	if len(cats) != 1 {
		t.Fatalf("list %+v", listed)
	}

	if st := h.json("POST", "/api/2.1/unity-catalog/schemas", pat, map[string]any{
		"name": "default", "catalog_name": "main",
	}, nil); st != 200 {
		t.Fatalf("schema %d", st)
	}

	var table map[string]any
	if st := h.json("POST", "/api/2.1/unity-catalog/tables", pat, map[string]any{
		"name": "t", "catalog_name": "main", "schema_name": "default",
		"table_type": "EXTERNAL", "data_source_format": "DELTA",
		"storage_location": "file:///tmp/t",
	}, &table); st != 200 {
		t.Fatalf("external table %d", st)
	}
	if table["table_id"] != "tbl-1" {
		t.Fatalf("table %+v", table)
	}
	if !json.Valid(lastBody) {
		t.Fatalf("upstream body %s", lastBody)
	}

	if st := h.json("GET", "/api/2.1/unity-catalog/tables/main.default.t", pat, nil, nil); st != 200 {
		t.Fatalf("get table %d", st)
	}

	if st := h.json("POST", "/api/2.1/unity-catalog/tables", pat, map[string]any{
		"name": "m", "catalog_name": "main", "schema_name": "default",
		"table_type": "MANAGED",
	}, nil); st != 501 {
		t.Fatalf("managed %d", st)
	}

	if st := h.json("GET", "/api/2.1/unity-catalog/permissions/catalog/main", pat, nil, nil); st != 501 {
		t.Fatalf("permissions get %d", st)
	}
	if st := h.json("PATCH", "/api/2.1/unity-catalog/grants/catalog/main", pat, map[string]any{
		"changes": []any{},
	}, nil); st != 501 {
		t.Fatalf("grants patch %d", st)
	}
}

func TestUnityCatalogUnreachableSidecarIs502(t *testing.T) {
	h := newHarness(t)
	h.srv.UC = uc.New("http://127.0.0.1:1", false, nil)
	if st := h.json("GET", "/api/2.1/unity-catalog/catalogs", h.srv.Store.AdminPAT, nil, nil); st != 502 {
		t.Fatalf("unreachable %d", st)
	}
}

func TestNewWiresUCClient(t *testing.T) {
	s, err := New(&config.Config{
		DataDir:    t.TempDir(),
		DisableTLS: true,
		UCURL:      "http://uc:8080",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.UC == nil || !s.UC.Attached() {
		t.Fatal("UCURL did not attach")
	}
}

func TestUCPathHelpers(t *testing.T) {
	if !isUCGrantPath("/api/2.1/unity-catalog/permissions/table/a.b.c") {
		t.Fatal("permissions")
	}
	if !isUCGrantPath("/api/2.1/unity-catalog/grants") {
		t.Fatal("grants")
	}
	if isUCGrantPath("/api/2.1/unity-catalog/catalogs") {
		t.Fatal("catalogs is not a grant")
	}
	if !isUCTablesCollection("/api/2.1/unity-catalog/tables") {
		t.Fatal("tables collection")
	}
	if isUCTablesCollection("/api/2.1/unity-catalog/tables/main.default.t") {
		t.Fatal("table instance")
	}
	if !isManagedTable([]byte(`{"table_type":"managed"}`)) {
		t.Fatal("managed")
	}
	if isManagedTable([]byte(`{"table_type":"EXTERNAL"}`)) {
		t.Fatal("external")
	}
	if isManagedTable([]byte(`not-json`)) {
		t.Fatal("garbage")
	}
}
