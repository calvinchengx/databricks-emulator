package server

import (
	"net/http"
	"testing"
)

// The rung this round earned the hard way.
//
// An unmodified databricks-sdk pages SCIM collections with a 1-based
// startIndex and stops when it receives a short page. A handler that ignores
// startIndex and echoes the whole list every time never terminates: the client
// asks for record 2, is handed record 1 again, appends it, asks for record 3,
// and hangs forever rather than failing.
//
// A "list returns the group" assertion passes against that handler. This one
// does not, which is the entire reason it exists.
func TestScimGroupsPagingTerminates(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT

	for _, name := range []string{"alpha", "beta", "gamma"} {
		res := h.do(http.MethodPost, "/api/2.0/preview/scim/v2/Groups", pat,
			map[string]any{"displayName": name})
		if res.StatusCode != http.StatusCreated {
			t.Fatalf("create %s: status %d", name, res.StatusCode)
		}
		res.Body.Close()
	}

	// Walk the collection the way the SDK does, one at a time.
	seen := map[string]bool{}
	for start, guard := 1, 0; ; guard++ {
		if guard > 10 {
			t.Fatal("paging never terminated: the handler is ignoring startIndex")
		}
		var page struct {
			TotalResults int `json:"totalResults"`
			StartIndex   int `json:"startIndex"`
			ItemsPerPage int `json:"itemsPerPage"`
			Resources    []struct {
				ID          string `json:"id"`
				DisplayName string `json:"displayName"`
			} `json:"Resources"`
		}
		h.json(http.MethodGet,
			"/api/2.0/preview/scim/v2/Groups?startIndex="+itoa(int64(start))+"&count=1",
			pat, nil, &page)

		if page.TotalResults != 3 {
			t.Fatalf("totalResults = %d, want 3", page.TotalResults)
		}
		if page.StartIndex != start {
			t.Fatalf("startIndex echoed as %d, want %d — the handler is not reading it",
				page.StartIndex, start)
		}
		if len(page.Resources) == 0 {
			break
		}
		for _, r := range page.Resources {
			if seen[r.ID] {
				t.Fatalf("group %s served twice; paging is not advancing", r.ID)
			}
			seen[r.ID] = true
		}
		start += len(page.Resources)
	}

	if len(seen) != 3 {
		t.Fatalf("walked %d groups, want 3", len(seen))
	}
}
