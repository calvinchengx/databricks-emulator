package hs2

import (
	"math"
	"strconv"
	"strings"
	"testing"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

// The regression: a small first value fixed the column at INT and every
// later value was truncated by the int32 conversion, with no error. The
// client received a wrong number and no way to know.
func TestWideValueDoesNotTruncateAfterASmallFirstRow(t *testing.T) {
	tab, err := parseStdout(`[[1],[5000000000]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tab.types[0] != cliservice.TTypeId_BIGINT_TYPE {
		t.Fatalf("column type = %v, want BIGINT_TYPE", tab.types[0])
	}
	if tab.cols[0].I32Val != nil {
		t.Fatal("column packed as int32; a value beyond int32 would be truncated")
	}
	got := tab.cols[0].I64Val.Values
	if len(got) != 2 || got[0] != 1 || got[1] != 5000000000 {
		t.Fatalf("values = %v, want [1 5000000000]", got)
	}
}

// Widening must survive whatever order the rows arrive in.
func TestWideValueFirstAlsoStaysBigint(t *testing.T) {
	tab, err := parseStdout(`[[5000000000],[1]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tab.types[0] != cliservice.TTypeId_BIGINT_TYPE {
		t.Fatalf("column type = %v, want BIGINT_TYPE", tab.types[0])
	}
	if got := tab.cols[0].I64Val.Values; got[0] != 5000000000 || got[1] != 1 {
		t.Fatalf("values = %v, want [5000000000 1]", got)
	}
}

// Nulls must not defeat the scan: the wide value sits behind one.
func TestNullsDoNotHideAWideValue(t *testing.T) {
	tab, err := parseStdout(`[[1],[null],[5000000000]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tab.types[0] != cliservice.TTypeId_BIGINT_TYPE {
		t.Fatalf("column type = %v, want BIGINT_TYPE", tab.types[0])
	}
	if got := tab.cols[0].I64Val.Values[2]; got != 5000000000 {
		t.Fatalf("value = %d, want 5000000000", got)
	}
}

// The int32 boundary itself: MaxInt32 still fits, one more does not.
func TestInt32Boundary(t *testing.T) {
	for _, c := range []struct {
		in   string
		want cliservice.TTypeId
	}{
		{"[[" + strconv.Itoa(math.MaxInt32) + "]]", cliservice.TTypeId_INT_TYPE},
		{"[[" + strconv.FormatInt(math.MaxInt32+1, 10) + "]]", cliservice.TTypeId_BIGINT_TYPE},
		{"[[" + strconv.Itoa(math.MinInt32) + "]]", cliservice.TTypeId_INT_TYPE},
		{"[[" + strconv.FormatInt(math.MinInt32-1, 10) + "]]", cliservice.TTypeId_BIGINT_TYPE},
	} {
		tab, err := parseStdout(c.in)
		if err != nil {
			t.Fatalf("parse %s: %v", c.in, err)
		}
		if tab.types[0] != c.want {
			t.Errorf("%s -> %v, want %v", c.in, tab.types[0], c.want)
		}
	}
}

// Ordinary values keep the narrow column: this must not promote everything.
func TestSmallValuesStayInt(t *testing.T) {
	tab, err := parseStdout(`[[1],[2],[3]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tab.types[0] != cliservice.TTypeId_INT_TYPE {
		t.Fatalf("column type = %v, want INT_TYPE", tab.types[0])
	}
}

// buildTable trusts an engine-declared schema and skips inferType, so the
// conversion itself must refuse an out-of-range value rather than truncate.
func TestEngineDeclaredIntRefusesAnOversizeValue(t *testing.T) {
	_, err := parseStdout(`{"schema":{"fields":[{"name":"n","type":"integer"}]},"data":[[5000000000]]}`)
	if err == nil {
		t.Fatal("an oversize value in an engine-declared INT column was accepted")
	}
	if !strings.Contains(err.Error(), "does not fit") {
		t.Fatalf("error = %q, want it to name the overflow", err)
	}
}

// The same declared column is untouched for values that do fit.
func TestEngineDeclaredIntAcceptsValuesThatFit(t *testing.T) {
	tab, err := parseStdout(`{"schema":{"fields":[{"name":"n","type":"integer"}]},"data":[[7],[-7]]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.cols[0].I32Val.Values; got[0] != 7 || got[1] != -7 {
		t.Fatalf("values = %v, want [7 -7]", got)
	}
}

// A declared BIGINT carries the wide value through untouched.
func TestEngineDeclaredBigintKeepsWideValues(t *testing.T) {
	tab, err := parseStdout(`{"schema":{"fields":[{"name":"n","type":"bigint"}]},"data":[[5000000000]]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := tab.cols[0].I64Val.Values[0]; got != 5000000000 {
		t.Fatalf("value = %d, want 5000000000", got)
	}
}

// A non-numeric value mixed into a numeric column still fails by name
// rather than being silently coerced — unchanged by this fix.
func TestMixedTypesStillFailLoudly(t *testing.T) {
	if _, err := parseStdout(`[[1],["abc"]]`); err == nil {
		t.Fatal("a string in an INT column was accepted")
	}
}
