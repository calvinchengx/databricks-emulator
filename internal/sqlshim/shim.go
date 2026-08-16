// Package sqlshim rewrites warehouse SQL that Unity Catalog OSS and Sail
// cannot run as Databricks would, into a form both can execute.
//
// CREATE TABLE cat.sch.t with no LOCATION is a managed table on a real
// workspace. UC OSS has no managed handshake Sail speaks (io.unitycatalog.tableId).
// This package allocates a filesystem location and emits an unqualified
// CREATE TABLE … LOCATION that Sail already writes; the server then
// registers an EXTERNAL table in UC OSS. hive_metastore and statements
// that already have LOCATION are left alone.
package sqlshim

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

const DefaultRoot = "file:///data/delta/managed"

// EmptyInformationSchema is engine stdout for Spark SQL information_schema
// queries. A bare [] has no column metadata; dbt-databricks then yields
// table=None and crashes iterating get_uc_tables.
const EmptyInformationSchema = `{"schema":{"fields":[{"name":"table_name","type":"string"},{"name":"table_type","type":"string"},{"name":"file_format","type":"string"},{"name":"table_owner","type":"string"},{"name":"databricks_table_type","type":"string"}]},"data":[]}`

// SchemaRef is a two-part schema name from CREATE SCHEMA.
type SchemaRef struct {
	Catalog string
	Schema  string
}

// ExternalTable is a UC EXTERNAL registration after a LOCATION write.
type ExternalTable struct {
	Catalog  string
	Schema   string
	Name     string
	Location string
}

// Plan is how runSQLStatement should execute one statement.
type Plan struct {
	SQL          string
	SkipEngine   bool
	EmptyJSON    bool // succeed with stdout "[]"
	CreateSchema *SchemaRef
	Register     *ExternalTable
}

var (
	blockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	lineComment  = regexp.MustCompile(`(?m)^\s*--.*$`)
	createTable  = regexp.MustCompile(`(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?`)
	createSchema = regexp.MustCompile(`(?is)^\s*CREATE\s+SCHEMA\s+(IF\s+NOT\s+EXISTS\s+)?`)
	// UC OSS does not serve Spark SQL information_schema.* (tables, row_filters, …).
	infoSchema  = regexp.MustCompile(`(?i)information_schema`)
	hasLocation = regexp.MustCompile(`(?i)\bLOCATION\s+`)
	temporary   = regexp.MustCompile(`(?is)^\s*CREATE\s+TEMPORARY\b`)
)

// Rewrite inspects one warehouse statement. root is the engine-visible URI
// prefix (no trailing slash), e.g. file:///data/delta/managed.
func Rewrite(sql, root string) Plan {
	if root == "" {
		root = DefaultRoot
	}
	root = strings.TrimRight(root, "/")
	src := stripLeadingComments(sql)
	if infoSchema.MatchString(src) {
		return Plan{SkipEngine: true, EmptyJSON: true}
	}
	if temporary.MatchString(src) {
		return Plan{SQL: sql}
	}
	if m := createSchema.FindStringIndex(src); m != nil {
		rest := strings.TrimSpace(src[m[1]:])
		ident := firstIdent(rest)
		parts := splitIdent(ident)
		if len(parts) == 2 && !strings.EqualFold(parts[0], "hive_metastore") {
			return Plan{SkipEngine: true, EmptyJSON: true, CreateSchema: &SchemaRef{Catalog: parts[0], Schema: parts[1]}}
		}
		return Plan{SQL: sql}
	}
	if loc := createTable.FindStringIndex(src); loc != nil {
		return rewriteCreateTable(sql, src, loc[1], root)
	}
	return Plan{SQL: sql}
}

func rewriteCreateTable(original, src string, nameStart int, root string) Plan {
	if hasLocation.MatchString(src) {
		return Plan{SQL: original}
	}
	rest := strings.TrimSpace(src[nameStart:])
	ident := firstIdent(rest)
	parts := splitIdent(ident)
	if len(parts) < 3 {
		return Plan{SQL: original}
	}
	if strings.EqualFold(parts[0], "hive_metastore") {
		return Plan{SQL: original}
	}
	catalog, schema, name := parts[0], parts[1], parts[2]
	loc := fmt.Sprintf("%s/%s/%s/%s", root, catalog, schema, name)
	body := strings.TrimSpace(rest[len(ident):])
	body = stripOrReplaceNoise(body)
	rewritten := "CREATE TABLE IF NOT EXISTS " + quoteIdent(name) + " " + injectLocation(body, loc)
	return Plan{
		SQL: rewritten,
		Register: &ExternalTable{
			Catalog:  catalog,
			Schema:   schema,
			Name:     name,
			Location: loc,
		},
	}
}

func stripOrReplaceNoise(body string) string {
	return strings.TrimSpace(body)
}

func injectLocation(body, loc string) string {
	clause := "USING delta LOCATION '" + loc + "'"
	as := regexp.MustCompile(`(?i)\sas\s`)
	idx := as.FindStringIndex(body)
	if idx == nil {
		if regexp.MustCompile(`(?i)\bUSING\b`).MatchString(body) {
			return strings.TrimSpace(body) + " LOCATION '" + loc + "'"
		}
		return strings.TrimSpace(body + " " + clause)
	}
	head, tail := strings.TrimSpace(body[:idx[0]]), body[idx[0]:]
	if regexp.MustCompile(`(?i)\bUSING\b`).MatchString(head) {
		return head + " LOCATION '" + loc + "' " + strings.TrimSpace(tail)
	}
	return head + " " + clause + " " + strings.TrimSpace(tail)
}

func stripLeadingComments(sql string) string {
	s := sql
	for {
		n := blockComment.ReplaceAllString(s, " ")
		n = lineComment.ReplaceAllString(n, " ")
		n = strings.TrimSpace(n)
		if n == s {
			return n
		}
		s = n
	}
}

func firstIdent(rest string) string {
	rest = strings.TrimSpace(rest)
	var b strings.Builder
	i := 0
	for i < len(rest) {
		c := rest[i]
		if c == '`' {
			j := strings.IndexByte(rest[i+1:], '`')
			if j < 0 {
				b.WriteString(rest[i:])
				break
			}
			b.WriteString(rest[i : i+j+2])
			i += j + 2
			continue
		}
		if c == '.' || isIdentChar(c) {
			b.WriteByte(c)
			i++
			continue
		}
		break
	}
	return strings.TrimSpace(b.String())
}

func isIdentChar(c byte) bool {
	return c == '_' || c == '-' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func splitIdent(ident string) []string {
	ident = strings.TrimSpace(ident)
	var parts []string
	i := 0
	for i < len(ident) {
		if ident[i] == '`' {
			j := strings.IndexByte(ident[i+1:], '`')
			if j < 0 {
				parts = append(parts, strings.Trim(ident[i:], "`"))
				break
			}
			parts = append(parts, ident[i+1:i+1+j])
			i += j + 2
			if i < len(ident) && ident[i] == '.' {
				i++
			}
			continue
		}
		j := strings.IndexByte(ident[i:], '.')
		if j < 0 {
			parts = append(parts, ident[i:])
			break
		}
		parts = append(parts, ident[i:i+j])
		i += j + 1
	}
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}

// ColumnsFromDescribe maps Sail/Spark DESCRIBE JSON into UC OSS column objects.
func ColumnsFromDescribe(stdout string) []map[string]any {
	raw := strings.TrimSpace(stdout)
	if raw == "" {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return nil
	}
	rows := describeMaps(v)
	var cols []map[string]any
	for i, row := range rows {
		name := stringField(row, "col_name", "column_name", "name")
		typ := stringField(row, "data_type", "type_name", "type")
		if name == "" || strings.HasPrefix(name, "#") {
			continue
		}
		spark := strings.ToLower(strings.TrimSpace(typ))
		ucType, typeJSON := sparkToUC(spark, name)
		cols = append(cols, map[string]any{
			"name":      name,
			"type_name": ucType,
			"type_text": spark,
			"type_json": typeJSON,
			"position":  i,
			"nullable":  true,
		})
	}
	return cols
}

func describeMaps(v any) []map[string]any {
	switch body := v.(type) {
	case []any:
		return rowsToMaps(body, nil)
	case map[string]any:
		return rowsToMaps(asArray(body["data"]), schemaFieldNames(body["schema"]))
	default:
		return nil
	}
}

func asArray(v any) []any {
	a, _ := v.([]any)
	return a
}

func schemaFieldNames(schema any) []string {
	obj, _ := schema.(map[string]any)
	fields, _ := obj["fields"].([]any)
	var names []string
	for _, f := range fields {
		fm, _ := f.(map[string]any)
		n, _ := fm["name"].(string)
		if n != "" {
			names = append(names, n)
		}
	}
	return names
}

func rowsToMaps(rows []any, names []string) []map[string]any {
	if len(rows) == 0 {
		return nil
	}
	if _, ok := rows[0].(map[string]any); ok {
		out := make([]map[string]any, 0, len(rows))
		for _, r := range rows {
			if m, ok := r.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	if names == nil {
		names = []string{"col_name", "data_type", "comment"}
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		arr, ok := r.([]any)
		if !ok {
			continue
		}
		m := map[string]any{}
		for i, n := range names {
			if i < len(arr) {
				m[n] = arr[i]
			}
		}
		out = append(out, m)
	}
	return out
}

func stringField(row map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := row[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func sparkToUC(spark, name string) (string, string) {
	kind, ucName := "string", "STRING"
	switch {
	case spark == "int" || spark == "integer" || spark == "int32":
		kind, ucName = "integer", "INT"
	case spark == "bigint" || spark == "long" || spark == "int64":
		kind, ucName = "long", "LONG"
	case spark == "double" || spark == "float" || spark == "real":
		kind, ucName = "double", "DOUBLE"
	case spark == "boolean" || spark == "bool":
		kind, ucName = "boolean", "BOOLEAN"
	case spark == "date":
		kind, ucName = "date", "DATE"
	case strings.HasPrefix(spark, "timestamp"):
		kind, ucName = "timestamp", "TIMESTAMP"
	case strings.HasPrefix(spark, "decimal"):
		kind, ucName = spark, "DECIMAL"
		if spark == "decimal" {
			kind = "decimal(10,0)"
		}
	}
	js, _ := json.Marshal(map[string]any{
		"name": name, "type": kind, "nullable": true, "metadata": map[string]any{},
	})
	return ucName, string(js)
}
