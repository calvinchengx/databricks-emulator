package hs2

import "strings"

// WarehouseID extracts the warehouse handle from a Databricks SQL HTTP path.
// Unknown shapes return false — they are not a silent session.
func WarehouseID(path string) (string, bool) {
	path = strings.TrimSuffix(path, "/")
	const endpoints = "/sql/1.0/endpoints/"
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
