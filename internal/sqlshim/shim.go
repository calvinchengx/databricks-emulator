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
//
// Err is set when a statement cannot be rewritten safely. SQL is left empty
// in that case, so a caller that forgets to check Err still cannot hand the
// engine a statement built from an unsafe name.
type Plan struct {
	SQL          string
	SkipEngine   bool
	EmptyJSON    bool // succeed with stdout "[]"
	CreateSchema *SchemaRef
	Register     *ExternalTable
	Err          string
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
	// Test the statement with its string literals blanked: a query whose
	// data merely mentions information_schema is a real query, and
	// answering it with an empty result set is a silent wrong answer.
	if infoSchema.MatchString(stripLiterals(src)) {
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
	// Backticked identifiers reach here verbatim, and every one of these
	// three is interpolated into a LOCATION string literal the engine will
	// parse. A quote would close that literal and leave the rest as SQL;
	// a ".." would walk out of the Delta root. Refuse instead of escaping:
	// a name that cannot be a path segment has no location to allocate.
	for label, part := range map[string]string{"catalog": catalog, "schema": schema, "table": name} {
		if !safeSegment(part) {
			return Plan{Err: fmt.Sprintf(
				"%s name %q cannot be used as a managed location; use letters, digits, '_', '-' or '.'",
				label, part)}
		}
	}
	loc := fmt.Sprintf("%s/%s/%s/%s", root, catalog, schema, name)
	body := strings.TrimSpace(rest[len(ident):])
	body = stripOrReplaceNoise(body)
	head := "CREATE TABLE IF NOT EXISTS "
	if regexp.MustCompile(`(?i)OR\s+REPLACE`).MatchString(src[:nameStart]) {
		head = "CREATE OR REPLACE TABLE "
	}
	rewritten := head + quoteIdent(name) + " " + injectLocation(body, loc)
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

// safeSegment reports whether an identifier can stand as one path segment
// inside a quoted LOCATION. This is the only thing between a backticked
// table name and the SQL the engine parses, so the set is a whitelist.
func safeSegment(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || c == '-' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

// stripLiterals blanks the contents of single-quoted strings, leaving the
// quotes so the statement still parses the same shape. Doubled quotes are
// the SQL escape and stay inside the literal.
func stripLiterals(s string) string {
	var b strings.Builder
	inLit := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			if inLit && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			inLit = !inLit
			b.WriteByte(c)
			continue
		}
		if !inLit {
			b.WriteByte(c)
		}
	}
	return b.String()
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
