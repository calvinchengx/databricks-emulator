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
