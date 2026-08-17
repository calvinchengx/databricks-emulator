package sqlshim

import (
	"strings"
	"testing"
)

const root = "file:///data/delta/managed"

// A quote inside a backticked name used to close the LOCATION literal and
// leave the remainder as SQL the engine would parse:
//
//	LOCATION 'file:///data/delta/managed/cat/sch/x' ; DROP TABLE victim; --'
func TestQuoteInNameCannotEscapeTheLocationLiteral(t *testing.T) {
	p := Rewrite("CREATE TABLE `cat`.`sch`.`x' ; DROP TABLE victim; --` (id INT)", root)
	if p.Err == "" {
		t.Fatalf("a name carrying a quote was accepted, producing: %s", p.SQL)
	}
	if p.SQL != "" {
		t.Fatalf("a refused rewrite must not hand the engine SQL: %q", p.SQL)
	}
	if p.Register != nil {
		t.Fatalf("a refused rewrite must not register a table: %+v", p.Register)
	}
}

// The same hole let a name walk out of the Delta root.
func TestDotDotCannotEscapeTheDeltaRoot(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE `cat`.`..`.`t` (id INT)",
		"CREATE TABLE `cat`.`sch`.`..` (id INT)",
		"CREATE TABLE `..`.`sch`.`t` (id INT)",
	} {
		p := Rewrite(sql, root)
		if p.Err == "" {
			t.Errorf("%s -> accepted, location %v", sql, p.Register)
		}
	}
}

// Anything that is not a usable path segment is refused rather than escaped.
func TestUnsafeSegmentsAreRefused(t *testing.T) {
	for _, name := range []string{"a/b", `a\b`, "a b", "a'b", `a"b`, "a\tb", "a\nb", "."} {
		p := Rewrite("CREATE TABLE `cat`.`sch`.`"+name+"` (id INT)", root)
		if p.Err == "" {
			t.Errorf("name %q accepted, SQL: %s", name, p.SQL)
		}
	}
}

// The refusal names the offending part so the caller can act on it.
func TestRefusalNamesTheOffendingPart(t *testing.T) {
	p := Rewrite("CREATE TABLE `cat`.`sch`.`bad name` (id INT)", root)
	if !strings.Contains(p.Err, "table") || !strings.Contains(p.Err, "bad name") {
		t.Fatalf("error %q should name the part and the value", p.Err)
	}
}

// Ordinary names must keep working, and land where they did before.
func TestOrdinaryNamesStillRewrite(t *testing.T) {
	p := Rewrite("CREATE TABLE `e2e`.`gold`.`orders` (id INT)", root)
	if p.Err != "" {
		t.Fatalf("ordinary name refused: %s", p.Err)
	}
	want := root + "/e2e/gold/orders"
	if p.Register == nil || p.Register.Location != want {
		t.Fatalf("location = %+v, want %s", p.Register, want)
	}
	if !strings.Contains(p.SQL, "LOCATION '"+want+"'") {
		t.Fatalf("SQL missing the location: %s", p.SQL)
	}
}

// Dots, dashes and underscores are legitimate inside a segment; only "." and
// ".." are traversal.
func TestDottedAndDashedNamesStillWork(t *testing.T) {
	for _, name := range []string{"a.b", "a-b", "a_b", "v1.2.3"} {
		p := Rewrite("CREATE TABLE `cat`.`sch`.`"+name+"` (id INT)", root)
		if p.Err != "" {
			t.Errorf("name %q refused: %s", name, p.Err)
		}
	}
}

// A query whose data merely mentions information_schema is a real query.
// Answering it with an empty result set is a silent wrong answer.
func TestInformationSchemaInsideALiteralIsNotACatalogQuery(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM events WHERE name = 'information_schema'",
		"INSERT INTO t VALUES ('information_schema')",
		"SELECT 'information_schema' AS s",
	} {
		p := Rewrite(sql, root)
		if p.SkipEngine || p.EmptyJSON {
			t.Errorf("%s -> skipped the engine", sql)
		}
		if p.SQL != sql {
			t.Errorf("%s -> rewritten to %q", sql, p.SQL)
		}
	}
}

// Real catalog queries must still be intercepted.
func TestRealInformationSchemaQueriesStillSkipTheEngine(t *testing.T) {
	for _, sql := range []string{
		"SELECT table_name FROM `system`.`information_schema`.`tables` WHERE table_catalog = 'e2e'",
		"SELECT * FROM `contoso`.`information_schema`.`row_filters` WHERE table_name = 'x'",
	} {
		p := Rewrite(sql, root)
		if !p.SkipEngine || !p.EmptyJSON {
			t.Errorf("%s -> reached the engine", sql)
		}
	}
}

// A doubled quote is the SQL escape and stays inside the literal, so the
// literal does not end early and the word after it is still data.
func TestDoubledQuoteStaysInsideTheLiteral(t *testing.T) {
	p := Rewrite("SELECT * FROM t WHERE s = 'it''s information_schema'", root)
	if p.SkipEngine {
		t.Fatalf("literal with a doubled quote treated as a catalog query: %+v", p)
	}
}
