package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The mux caps every body it routes. Without a cap these handlers buffer an
// attacker-sized upload into memory.
func TestBodyLimitPicksCeilingByRoute(t *testing.T) {
	cases := []struct {
		path string
		want int64
	}{
		{"/api/2.0/fs/files/a.txt", MaxUploadBody},
		{"/api/2.0/workspace-files/import-file/a.py", MaxUploadBody},
		{"/api/2.0/dbfs/put", MaxJSONBody},
		{"/api/2.1/jobs/create", MaxJSONBody},
		{"/api/2.0/fs/directories/d", MaxJSONBody},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodPost, c.path, nil)
		if got := bodyLimit(req); got != c.want {
			t.Errorf("bodyLimit(%s) = %d, want %d", c.path, got, c.want)
		}
	}
}

// An oversize raw upload is refused, not silently truncated onto disk.
func TestUploadOverCeilingIs413(t *testing.T) {
	h := newHarness(t)
	body := make([]byte, MaxUploadBody+1)
	resp := h.do("PUT", "/api/2.0/fs/files/big.bin", h.srv.Store.AdminPAT, body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	// The truncated prefix must not have been written.
	st := h.json("GET", "/api/2.0/dbfs/get-status?path=/big.bin", h.srv.Store.AdminPAT, nil, nil)
	if st == http.StatusOK {
		t.Fatal("truncated upload was persisted")
	}
}

// An upload under the ceiling still round-trips.
func TestUploadUnderCeilingSucceeds(t *testing.T) {
	h := newHarness(t)
	body := []byte(strings.Repeat("x", 1<<20))
	resp := h.do("PUT", "/api/2.0/fs/files/ok.bin", h.srv.Store.AdminPAT, body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put status = %d, want 204", resp.StatusCode)
	}
	var out map[string]any
	if st := h.json("GET", "/api/2.0/dbfs/get-status?path=/ok.bin", h.srv.Store.AdminPAT, nil, &out); st != http.StatusOK {
		t.Fatalf("status = %d, want 200", st)
	}
	if size, _ := out["file_size"].(float64); int64(size) != int64(len(body)) {
		t.Fatalf("file_size = %v, want %d", out["file_size"], len(body))
	}
}

// An oversize JSON control-plane body is refused with the same 413.
func TestJSONBodyOverCeilingIs413(t *testing.T) {
	h := newHarness(t)
	raw := `{"path":"/a.txt","contents":"` + strings.Repeat("A", int(MaxJSONBody)+1) + `"}`
	resp := h.do("POST", "/api/2.0/dbfs/put", h.srv.Store.AdminPAT, raw)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}
