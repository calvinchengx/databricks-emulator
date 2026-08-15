package server

import (
	"math"
	"strconv"
	"testing"
)

// parseMaxResults must be exact on every platform. The bug it replaces was
// int64 -> int narrowing: on a 32-bit build, 2^31 wraps negative and the
// caller's ceiling silently becomes the store's default.
func TestParseMaxResultsClampsBeforeNarrowing(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{"absent", "", 0},
		{"unparseable", "many", 0},
		{"zero means store default", "0", 0},
		{"negative means store default", "-1", 0},
		{"ordinary value passes through", "250", 250},
		{"at the cap", strconv.Itoa(maxResultsCap), maxResultsCap},
		{"just over the cap", strconv.Itoa(maxResultsCap + 1), maxResultsCap},
		{"surrounding whitespace", "  42  ", 42},
		// The values that made the old conversion lossy on a 32-bit int.
		{"math.MaxInt32 + 1", strconv.FormatInt(math.MaxInt32+1, 10), maxResultsCap},
		{"math.MaxUint32 + 100", strconv.FormatInt(math.MaxUint32+100, 10), maxResultsCap},
		{"math.MaxInt64", strconv.FormatInt(math.MaxInt64, 10), maxResultsCap},
		{"overflows int64 entirely", "99999999999999999999", 0},
		{"math.MinInt64", strconv.FormatInt(math.MinInt64, 10), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseMaxResults(c.in); got != c.want {
				t.Fatalf("parseMaxResults(%q) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

// Whatever a caller asks for, the result is a sane positive bound or the
// store's default — never a negative one smuggled in by narrowing.
func TestParseMaxResultsIsNeverNegative(t *testing.T) {
	for _, in := range []string{
		"-1", "-2147483648", strconv.FormatInt(math.MinInt64, 10),
		strconv.FormatInt(math.MaxInt64, 10), "4294967296", "not-a-number",
	} {
		if got := parseMaxResults(in); got < 0 {
			t.Fatalf("parseMaxResults(%q) = %d, want >= 0", in, got)
		}
	}
}
