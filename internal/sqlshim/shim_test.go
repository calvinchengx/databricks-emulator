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
	if strings.Contains(strings.ToUpper(p.SQL), "OR REPLACE") {
		t.Fatalf("left OR REPLACE: %s", p.SQL)
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
	unclosed := Rewrite("CREATE TABLE `e2e`.`s`.`t AS SELECT 1", "")
	if unclosed.SQL == "" {
		t.Fatal("unclosed ident")
	}
	dots := Rewrite("CREATE TABLE e2e..empty USING delta AS SELECT 1", "")
	if dots.SQL == "" {
		t.Fatal("dot ident")
	}
}

func TestColumnsFromDescribe(t *testing.T) {
	if ColumnsFromDescribe("") != nil || ColumnsFromDescribe("{") != nil {
		t.Fatal("empty/invalid")
	}
	env := ColumnsFromDescribe(`{"data":[{"name":"id","type":"bigint"},{"col_name":"# Partition","data_type":"int"},{"col_name":"amt","data_type":"decimal(19,4)"},{"col_name":"ok","data_type":"boolean"},{"col_name":"d","data_type":"date"},{"col_name":"ts","data_type":"timestamp"},{"col_name":"x","data_type":"double"},{"col_name":"i","data_type":"int"}]}`)
	want := map[string]string{"id": "LONG", "amt": "DECIMAL", "ok": "BOOLEAN", "d": "DATE", "ts": "TIMESTAMP", "x": "DOUBLE", "i": "INT"}
	got := map[string]string{}
	for _, c := range env {
		got[c["name"].(string)] = c["type_name"].(string)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("got %#v want %s=%s", got, k, v)
		}
	}
	if _, ok := got["# Partition"]; ok {
		t.Fatal("kept partition meta row")
	}
	skip := ColumnsFromDescribe(`[{"col_name":1,"data_type":"int"}]`)
	if len(skip) != 0 {
		t.Fatalf("non-string name: %+v", skip)
	}
	arr := ColumnsFromDescribe(`[{"col_name":"id","data_type":"int"},{"col_name":"name","data_type":"string"}]`)
	if len(arr) != 2 || arr[0]["type_name"] != "INT" || arr[1]["type_name"] != "STRING" {
		t.Fatalf("%+v", arr)
	}
	grid := ColumnsFromDescribe(`{"schema":{"fields":[{"name":"col_name"},{"name":"data_type"},{"name":"comment"}]},"data":[["id","int",""],["name","string",null]]}`)
	if len(grid) != 2 || grid[0]["name"] != "id" || grid[0]["type_name"] != "INT" || grid[1]["name"] != "name" {
		t.Fatalf("grid %+v", grid)
	}
	bare := ColumnsFromDescribe(`[["id","int"],["ts","timestamp"]]`)
	if len(bare) != 2 || bare[0]["name"] != "id" || bare[1]["type_name"] != "TIMESTAMP" {
		t.Fatalf("bare %+v", bare)
	}
	if ColumnsFromDescribe("1") != nil {
		t.Fatal("scalar")
	}
	if ColumnsFromDescribe(`{"data":[1,["ok","boolean"]]}`) == nil {
		t.Fatal("mixed data rows")
	}
	if ColumnsFromDescribe(`{"schema":{"fields":[null,{"name":""},{"name":"col_name"}]},"data":[]}`) != nil {
		t.Fatal("empty data")
	}
}
