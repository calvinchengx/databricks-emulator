package hs2

import (
	"fmt"
	"strings"
	"testing"
)

// The regression: rows decode into map[string]any, and the old code tried to
// recover order by marshalling that map back to JSON. encoding/json sorts map
// keys, so the "recovered" order was alphabetical and the engine's own column
// order was lost. SELECT zebra, apple reported (apple, zebra).
func TestColumnOrderFollowsTheEngineNotTheAlphabet(t *testing.T) {
	tab, err := parseStdout(`[{"zebra":1,"apple":2}]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.names; len(got) != 2 || got[0] != "zebra" || got[1] != "apple" {
		t.Fatalf("names = %v, want [zebra apple]", got)
	}
	// The values must travel with their own column, not just the labels.
	if v := tab.cols[0].I32Val.Values[0]; v != 1 {
		t.Fatalf("zebra = %d, want 1", v)
	}
	if v := tab.cols[1].I32Val.Values[0]; v != 2 {
		t.Fatalf("apple = %d, want 2", v)
	}
}

// Same rule inside the envelope shape.
func TestEnvelopeObjectRowsKeepEngineOrder(t *testing.T) {
	tab, err := parseStdout(`{"schema":{"fields":[]},"data":[{"zebra":1,"apple":2,"mango":3}]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.names; len(got) != 3 || got[0] != "zebra" || got[1] != "apple" || got[2] != "mango" {
		t.Fatalf("names = %v, want [zebra apple mango]", got)
	}
}

// Order that already is alphabetical must survive unchanged — the fix must
// not merely reverse or shuffle.
func TestAlreadyAlphabeticalOrderIsPreserved(t *testing.T) {
	tab, err := parseStdout(`[{"apple":1,"mango":2,"zebra":3}]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.names; len(got) != 3 || got[0] != "apple" || got[1] != "mango" || got[2] != "zebra" {
		t.Fatalf("names = %v, want [apple mango zebra]", got)
	}
}

// Many columns: map iteration would scramble these, and the count is high
// enough that passing by luck is implausible.
func TestWideRowKeepsOrder(t *testing.T) {
	var b strings.Builder
	var want []string
	b.WriteString(`[{`)
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("c%02d", 11-i) // deliberately reverse-sorted
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `%q:%d`, name, i)
		want = append(want, name)
	}
	b.WriteString(`}]`)
	tab, err := parseStdout(b.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(tab.names) != len(want) {
		t.Fatalf("names = %v, want %v", tab.names, want)
	}
	for i := range want {
		if tab.names[i] != want[i] {
			t.Fatalf("names = %v, want %v", tab.names, want)
		}
	}
	// Each value must still sit under its own name.
	for i := range want {
		if got := tab.cols[i].I32Val.Values[0]; int(got) != i {
			t.Fatalf("column %s = %d, want %d", tab.names[i], got, i)
		}
	}
}

// Later rows are read by name, so a row that lists its keys in a different
// order still lands in the right columns.
func TestLaterRowsWithDifferentKeyOrderStillAlign(t *testing.T) {
	tab, err := parseStdout(`[{"zebra":1,"apple":2},{"apple":20,"zebra":10}]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.names; got[0] != "zebra" || got[1] != "apple" {
		t.Fatalf("names = %v, want [zebra apple]", got)
	}
	if got := tab.cols[0].I32Val.Values; got[0] != 1 || got[1] != 10 {
		t.Fatalf("zebra column = %v, want [1 10]", got)
	}
	if got := tab.cols[1].I32Val.Values; got[0] != 2 || got[1] != 20 {
		t.Fatalf("apple column = %v, want [2 20]", got)
	}
}

// A row whose keys cannot be recovered from source falls back to sorted
// order — stable, never the random map iteration the old code could hit.
func TestFallbackIsSortedNotRandom(t *testing.T) {
	first := map[string]any{"zebra": 1, "apple": 2, "mango": 3}
	rows := []any{first}
	for i := 0; i < 20; i++ {
		tab, err := tableFromObjects(rows, first, nil)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := tab.names; got[0] != "apple" || got[1] != "mango" || got[2] != "zebra" {
			t.Fatalf("names = %v, want sorted [apple mango zebra]", got)
		}
	}
}

// A mismatched order hint (stale or wrong keys) must not be trusted blindly.
func TestOrderHintThatDoesNotMatchIsIgnored(t *testing.T) {
	first := map[string]any{"zebra": 1, "apple": 2}
	tab, err := tableFromObjects([]any{first}, first, []string{"zebra", "nonexistent"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if got := tab.names; got[0] != "apple" || got[1] != "zebra" {
		t.Fatalf("names = %v, want the sorted fallback [apple zebra]", got)
	}
}

// Array-shaped rows carry their order positionally and must be untouched.
func TestArrayRowsUnaffected(t *testing.T) {
	tab, err := parseStdout(`{"schema":{"fields":[{"name":"zebra","type":"integer"},{"name":"apple","type":"integer"}]},"data":[[1,2]]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.names; got[0] != "zebra" || got[1] != "apple" {
		t.Fatalf("names = %v, want [zebra apple]", got)
	}
}
