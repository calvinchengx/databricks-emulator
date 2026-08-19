package hs2

import (
	"testing"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

func TestParseStdoutObjects(t *testing.T) {
	tab, err := parseStdout(`[{"1":1}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(tab.names) != 1 || tab.types[0] != cliservice.TTypeId_INT_TYPE {
		t.Fatalf("schema %+v %+v", tab.names, tab.types)
	}
	if tab.cols[0].I32Val == nil || len(tab.cols[0].I32Val.Values) != 1 || tab.cols[0].I32Val.Values[0] != 1 {
		t.Fatalf("col %+v", tab.cols[0])
	}
}

func TestParseStdoutEnvelope(t *testing.T) {
	tab, err := parseStdout(`{"schema":{"fields":[{"name":"1","type":"integer"}]},"data":[[1]]}`)
	if err != nil {
		t.Fatal(err)
	}
	if tab.names[0] != "1" || tab.types[0] != cliservice.TTypeId_INT_TYPE {
		t.Fatalf("schema %+v %+v", tab.names, tab.types)
	}
	if tab.cols[0].I32Val.Values[0] != 1 {
		t.Fatalf("value %+v", tab.cols[0])
	}
}

func TestParseStdoutRefusesGarbage(t *testing.T) {
	if _, err := parseStdout("not a table"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := parseStdout(""); err == nil {
		t.Fatal("empty must fail")
	}
}

func TestEmptyJSONRowSetColumnsNotNil(t *testing.T) {
	tab, err := parseStdout("[]")
	if err != nil {
		t.Fatal(err)
	}
	rs := tab.rowSet()
	if rs.Columns == nil {
		t.Fatal("nil Columns: databricks-sql-connector iterates the list")
	}
	if len(rs.Columns) != 0 {
		t.Fatalf("want 0 columns, got %d", len(rs.Columns))
	}
}

func TestWarehouseID(t *testing.T) {
	id, ok := WarehouseID("/sql/1.0/endpoints/wh-1")
	if !ok || id != "wh-1" {
		t.Fatalf("endpoints %q %v", id, ok)
	}
	id, ok = WarehouseID("/sql/protocolv1/o/1/wh-2")
	if !ok || id != "wh-2" {
		t.Fatalf("protocolv1 %q %v", id, ok)
	}
	if _, ok := WarehouseID("/sql/nope"); ok {
		t.Fatal("unknown path")
	}
}

// The builder and the parser are one fact spelled twice; this is what keeps
// them the same fact. dbt_task generated "/sql/1.0/warehouses/{id}" for months
// and no unit test noticed, because nothing asserted that a path the emulator
// HANDS OUT is a path it ACCEPTS.
func TestWarehousePathRoundTrips(t *testing.T) {
	for _, id := range []string{"wh-1", "abc123", "wh-a-b-c", "0123456789abcdef"} {
		path := WarehousePath(id)
		got, ok := WarehouseID(path)
		if !ok {
			t.Fatalf("WarehouseID(%q) rejected a path WarehousePath built", path)
		}
		if got != id {
			t.Fatalf("round trip: WarehousePath(%q) -> %q -> %q", id, path, got)
		}
	}
}

// The spelling that was wrong, named so a future edit back to it fails here
// rather than in an e2e nobody runs locally.
func TestWarehousesSpellingIsNotAPath(t *testing.T) {
	if _, ok := WarehouseID("/sql/1.0/warehouses/wh-1"); ok {
		t.Fatal("/sql/1.0/warehouses/ parsed; the router serves /sql/1.0/endpoints/ only, " +
			"so accepting it here would hide a profile that cannot connect")
	}
}
