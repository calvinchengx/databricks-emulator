package sqlshim

import (
	"strings"
	"testing"
)

func TestRewriteUCCreateTableAllocatesLocation(t *testing.T) {
	sql := "CREATE OR REPLACE TABLE `contoso`.`gold`.`dim_customer` USING delta AS SELECT 1 AS id"
	p := Rewrite(sql, "")
	if p.Register == nil {
		t.Fatalf("expected register: %+v", p)
	}
	if p.Register.Catalog != "contoso" || p.Register.Schema != "gold" || p.Register.Name != "dim_customer" {
		t.Fatalf("register %+v", p.Register)
	}
	wantLoc := "file:///data/delta/managed/contoso/gold/dim_customer"
	if p.Register.Location != wantLoc {
		t.Fatalf("location %s", p.Register.Location)
	}
	if !strings.Contains(strings.ToUpper(p.SQL), "OR REPLACE") {
		t.Fatalf("Sail needs OR REPLACE to overwrite: %s", p.SQL)
	}
	if !strings.Contains(p.SQL, "LOCATION '"+wantLoc+"'") {
		t.Fatalf("missing LOCATION: %s", p.SQL)
	}
	if strings.Contains(p.SQL, "`contoso`") || strings.Contains(p.SQL, "contoso`.`gold") {
		t.Fatalf("must unqualify for Sail: %s", p.SQL)
	}
	if !strings.Contains(p.SQL, "AS SELECT") && !strings.Contains(strings.ToLower(p.SQL), " as select") {
		t.Fatalf("lost CTAS: %s", p.SQL)
	}
}

func TestRewriteLeavesHiveMetastoreAndLocationAlone(t *testing.T) {
	hive := "create or replace table `hive_metastore`.`default`.`one` using delta as select 1 as id"
	p := Rewrite(hive, "")
	if p.SQL != hive || p.Register != nil {
		t.Fatalf("hive rewritten: %+v", p)
	}
	withLoc := "CREATE TABLE events (id INT) USING delta LOCATION 'file:///data/delta/e2e/events'"
	p = Rewrite(withLoc, "")
	if p.SQL != withLoc || p.Register != nil {
		t.Fatalf("location rewritten: %+v", p)
	}
	plain := "CREATE TABLE one AS SELECT 1 AS id"
	p = Rewrite(plain, "")
	if p.SQL != plain || p.Register != nil {
		t.Fatalf("unqualified rewritten: %+v", p)
	}
}

func TestRewriteRowFiltersAndCreateSchema(t *testing.T) {
	p := Rewrite("SELECT * FROM `contoso`.`information_schema`.`row_filters` WHERE table_name = 'x'", "")
	if !p.SkipEngine || !p.EmptyJSON {
		t.Fatalf("row_filters: %+v", p)
	}
	p = Rewrite("SELECT table_name FROM `system`.`information_schema`.`tables` WHERE table_catalog = 'e2e'", "")
	if !p.SkipEngine || !p.EmptyJSON {
		t.Fatalf("information_schema.tables: %+v", p)
	}
	p = Rewrite("create schema if not exists `contoso`.`gold`", "")
	if p.CreateSchema == nil || p.CreateSchema.Catalog != "contoso" || p.CreateSchema.Schema != "gold" || !p.SkipEngine {
		t.Fatalf("schema: %+v", p)
	}
	p = Rewrite("create schema if not exists `hive_metastore`.`default`", "")
	if p.CreateSchema != nil || p.SkipEngine {
		t.Fatalf("hive schema: %+v", p)
	}
}

func TestRewriteStripsDbtComment(t *testing.T) {
	sql := "/* {\"app\":\"dbt\"} */\nCREATE TABLE `e2e`.`s`.`from_shim` USING delta AS SELECT 1 AS id"
	p := Rewrite(sql, "file:///data/delta/managed")
	if p.Register == nil || p.Register.Name != "from_shim" {
		t.Fatalf("%+v", p)
	}
}

func TestRewriteRemainingBranches(t *testing.T) {
	if p := Rewrite("CREATE TEMPORARY TABLE t AS SELECT 1", ""); p.SQL != "CREATE TEMPORARY TABLE t AS SELECT 1" || p.Register != nil {
		t.Fatalf("temporary: %+v", p)
	}
	if p := Rewrite("CREATE SCHEMA gold", ""); p.CreateSchema != nil || p.SQL != "CREATE SCHEMA gold" {
		t.Fatalf("one-part schema: %+v", p)
	}
	if p := Rewrite("SELECT 1", ""); p.SQL != "SELECT 1" || p.SkipEngine {
		t.Fatalf("passthrough: %+v", p)
	}
	ddl := "CREATE TABLE e2e.s.cols (id INT) USING delta"
	p := Rewrite(ddl, "file:///data/delta/managed/")
	if p.Register == nil || !strings.Contains(p.SQL, "LOCATION 'file:///data/delta/managed/e2e/s/cols'") {
		t.Fatalf("using no as: %+v sql=%s", p.Register, p.SQL)
	}
	noUsing := "CREATE TABLE e2e.s.plain (id INT)"
	p = Rewrite(noUsing, "")
	if p.Register == nil || !strings.Contains(p.SQL, "USING delta LOCATION") {
		t.Fatalf("no using: %s", p.SQL)
	}
	asOnly := "CREATE TABLE e2e.s.asonly AS SELECT 1 AS id"
	p = Rewrite(asOnly, "")
	if p.Register == nil || !strings.Contains(strings.ToLower(p.SQL), "using delta location") {
		t.Fatalf("as no using: %s", p.SQL)
	}
	// An unterminated backtick swallows the rest of the statement as the
	// table name. That used to be rewritten into a table literally named
	// "t AS SELECT 1"; it is now refused, because a name carrying spaces
	// cannot be a location segment.
	unclosed := Rewrite("CREATE TABLE `e2e`.`s`.`t AS SELECT 1", "")
	if unclosed.Err == "" {
		t.Fatalf("unclosed ident should be refused, got SQL %q", unclosed.SQL)
	}
	if unclosed.SQL != "" {
		t.Fatalf("a refused rewrite must not hand the engine SQL: %q", unclosed.SQL)
	}
	dots := Rewrite("CREATE TABLE e2e..empty USING delta AS SELECT 1", "")
	if dots.SQL == "" {
		t.Fatal("dot ident")
	}
}
