package hs2

import "strings"

// endpoints is the one spelling of a warehouse's HTTP path. The parser below
// and the builder beside it both read this, so the two cannot disagree.
const endpoints = "/sql/1.0/endpoints/"

// WarehousePath builds the HTTP path a SQL client uses to reach warehouse id.
//
// It lives next to the parser deliberately. The path was previously spelled out
// wherever one was needed, and dbt_task spelled it "/sql/1.0/warehouses/{id}" —
// a form no route serves and WarehouseID rejects — so every generated dbt
// profile pointed somewhere the emulator does not listen. Building through here
// makes that a compile-time impossibility rather than a runtime connection
// refusal, and TestWarehousePathRoundTrips holds the pair together.
func WarehousePath(id string) string { return endpoints + id }

// WarehouseID extracts the warehouse handle from a Databricks SQL HTTP path.
// Unknown shapes return false — they are not a silent session.
func WarehouseID(path string) (string, bool) {
	path = strings.TrimSuffix(path, "/")
	if rest, ok := strings.CutPrefix(path, endpoints); ok {
		if rest != "" && !strings.Contains(rest, "/") {
			return rest, true
		}
		return "", false
	}
	const proto = "/sql/protocolv1/o/"
	if rest, ok := strings.CutPrefix(path, proto); ok {
		org, id, found := strings.Cut(rest, "/")
		if found && org != "" && id != "" && !strings.Contains(id, "/") {
			return id, true
		}
	}
	return "", false
}
