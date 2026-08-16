package hs2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

// table is one COLUMN_BASED_SET result. Built only from engine stdout.
type table struct {
	names []string
	types []cliservice.TTypeId
	cols  []*cliservice.TColumn
}

func parseStdout(stdout string) (*table, error) {
	raw := strings.TrimSpace(stdout)
	if raw == "" {
		return nil, fmt.Errorf("engine returned no rows: empty stdout")
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("engine stdout is not JSON: %w\n%s", err, raw)
	}
	switch body := v.(type) {
	case []any:
		if len(body) == 0 {
			return emptyTable(), nil
		}
		if obj, ok := body[0].(map[string]any); ok {
			return tableFromObjects(body, obj)
		}
		if _, ok := body[0].([]any); ok {
			return tableFromArrays(body, nil)
		}
		return tableFromArrays([]any{body}, nil)
	case map[string]any:
		data, ok := body["data"]
		if !ok {
			return nil, fmt.Errorf("engine stdout is not a row array: %s", raw)
		}
		rows, ok := data.([]any)
		if !ok {
			return nil, fmt.Errorf("engine envelope data is not an array: %s", raw)
		}
		names, types := schemaFromEnvelope(body["schema"])
		if len(rows) == 0 {
			if len(names) == 0 {
				return emptyTable(), nil
			}
			return tableFromArrays(nil, &colSpec{names: names, types: types})
		}
		if obj, ok := rows[0].(map[string]any); ok {
			return tableFromObjects(rows, obj)
		}
		return tableFromArrays(rows, &colSpec{names: names, types: types})
	default:
		return nil, fmt.Errorf("engine stdout is not a row array: %s", raw)
	}
}

type colSpec struct {
	names []string
	types []cliservice.TTypeId
}

func emptyTable() *table {
	return &table{}
}

func schemaFromEnvelope(schema any) ([]string, []cliservice.TTypeId) {
	obj, ok := schema.(map[string]any)
	if !ok {
		return nil, nil
	}
	fields, ok := obj["fields"].([]any)
	if !ok {
		return nil, nil
	}
	var names []string
	var types []cliservice.TTypeId
	for _, f := range fields {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		name, _ := fm["name"].(string)
		if name == "" {
			name = fmt.Sprintf("_c%d", len(names))
		}
		names = append(names, name)
		types = append(types, sparkType(fm["type"]))
	}
	return names, types
}

func sparkType(v any) cliservice.TTypeId {
	s, _ := v.(string)
	switch strings.ToLower(s) {
	case "boolean":
		return cliservice.TTypeId_BOOLEAN_TYPE
	case "byte", "tinyint":
		return cliservice.TTypeId_TINYINT_TYPE
	case "short", "smallint":
		return cliservice.TTypeId_SMALLINT_TYPE
	case "integer", "int":
		return cliservice.TTypeId_INT_TYPE
	case "long", "bigint":
		return cliservice.TTypeId_BIGINT_TYPE
	case "float", "double":
		return cliservice.TTypeId_DOUBLE_TYPE
	default:
		return cliservice.TTypeId_STRING_TYPE
	}
}

func tableFromObjects(rows []any, first map[string]any) (*table, error) {
	names := make([]string, 0, len(first))
	for k := range first {
		names = append(names, k)
	}
	// Stable order: first object's key iteration is random. Sort by appearance
	// in the first object via a second pass over a json decoder? For SELECT 1
	// there is one key. Keep map iteration for the first row then reuse.
	if len(names) > 1 {
		// Re-read first object keys in JSON order by encoding then scanning.
		raw, err := json.Marshal(first)
		if err == nil {
			var ordered []string
			dec := json.NewDecoder(bytes.NewReader(raw))
			if tok, err := dec.Token(); err == nil && tok == json.Delim('{') {
				for dec.More() {
					k, err := dec.Token()
					if err != nil {
						break
					}
					if ks, ok := k.(string); ok {
						ordered = append(ordered, ks)
					}
					var skip any
					_ = dec.Decode(&skip)
				}
			}
			if len(ordered) == len(names) {
				names = ordered
			}
		}
	}
	grid := make([][]any, len(rows))
	for i, row := range rows {
		obj, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("row %d is not an object", i)
		}
		vals := make([]any, len(names))
		for j, name := range names {
			vals[j] = obj[name]
		}
		grid[i] = vals
	}
	return buildTable(names, nil, grid)
}

func tableFromArrays(rows []any, spec *colSpec) (*table, error) {
	width := 0
	grid := make([][]any, len(rows))
	for i, row := range rows {
		arr, ok := row.([]any)
		if !ok {
			return nil, fmt.Errorf("row %d is not an array", i)
		}
		if len(arr) > width {
			width = len(arr)
		}
		grid[i] = arr
	}
	if spec != nil && len(spec.names) > width {
		width = len(spec.names)
	}
	names := make([]string, width)
	var types []cliservice.TTypeId
	if spec != nil {
		types = spec.types
		for i := 0; i < width; i++ {
			if i < len(spec.names) && spec.names[i] != "" {
				names[i] = spec.names[i]
			} else {
				names[i] = fmt.Sprintf("_c%d", i)
			}
		}
	} else {
		for i := range names {
			names[i] = fmt.Sprintf("_c%d", i)
		}
	}
	return buildTable(names, types, grid)
}

func buildTable(names []string, hinted []cliservice.TTypeId, grid [][]any) (*table, error) {
	n := len(names)
	if n == 0 {
		return emptyTable(), nil
	}
	kinds := make([]cliservice.TTypeId, n)
	for i := range kinds {
		if i < len(hinted) {
			kinds[i] = hinted[i]
			continue
		}
		kinds[i] = inferType(grid, i)
	}
	cols := make([]*cliservice.TColumn, n)
	for i := 0; i < n; i++ {
		col, err := packColumn(kinds[i], grid, i)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	return &table{names: names, types: kinds, cols: cols}, nil
}

// inferType reads the whole column, not just its first non-null value. One
// small number at the top used to fix the column at INT, and every later
// value wider than int32 was then silently truncated by packColumn.
func inferType(grid [][]any, col int) cliservice.TTypeId {
	kind := cliservice.TTypeId_STRING_TYPE
	found := false
	for _, row := range grid {
		if col >= len(row) || row[col] == nil {
			continue
		}
		got := valueType(row[col])
		if !found {
			kind, found = got, true
			continue
		}
		// Widen INT to BIGINT the moment one value needs 64 bits. Other
		// mixes are left alone: packColumn still refuses them by name,
		// which is a loud failure rather than a wrong number.
		if kind == cliservice.TTypeId_INT_TYPE && got == cliservice.TTypeId_BIGINT_TYPE {
			kind = got
		}
	}
	return kind
}

func valueType(v any) cliservice.TTypeId {
	switch n := v.(type) {
	case bool:
		return cliservice.TTypeId_BOOLEAN_TYPE
	case json.Number:
		if _, err := n.Int64(); err == nil {
			if i, err := strconv.ParseInt(n.String(), 10, 32); err == nil && strconv.FormatInt(i, 10) == n.String() {
				return cliservice.TTypeId_INT_TYPE
			}
			return cliservice.TTypeId_BIGINT_TYPE
		}
		return cliservice.TTypeId_DOUBLE_TYPE
	case float64:
		if n == float64(int64(n)) {
			if n >= -2147483648 && n <= 2147483647 {
				return cliservice.TTypeId_INT_TYPE
			}
			return cliservice.TTypeId_BIGINT_TYPE
		}
		return cliservice.TTypeId_DOUBLE_TYPE
	default:
		return cliservice.TTypeId_STRING_TYPE
	}
}

func packColumn(kind cliservice.TTypeId, grid [][]any, col int) (*cliservice.TColumn, error) {
	nulls := make([]bool, len(grid))
	out := &cliservice.TColumn{}
	switch kind {
	case cliservice.TTypeId_BOOLEAN_TYPE:
		vals := make([]bool, len(grid))
		for i, row := range grid {
			if col >= len(row) || row[col] == nil {
				nulls[i] = true
				continue
			}
			b, ok := row[col].(bool)
			if !ok {
				return nil, fmt.Errorf("column %d row %d is not bool: %T", col, i, row[col])
			}
			vals[i] = b
		}
		out.BoolVal = &cliservice.TBoolColumn{Values: vals, Nulls: packNulls(nulls)}
	case cliservice.TTypeId_INT_TYPE:
		vals := make([]int32, len(grid))
		for i, row := range grid {
			if col >= len(row) || row[col] == nil {
				nulls[i] = true
				continue
			}
			n, err := asInt64(row[col])
			if err != nil {
				return nil, fmt.Errorf("column %d row %d: %w", col, i, err)
			}
			// Reachable when the engine's own schema declared INT: buildTable
			// trusts that hint and never calls inferType. Refuse rather than
			// hand the client a truncated number.
			if n > math.MaxInt32 || n < math.MinInt32 {
				return nil, fmt.Errorf("column %d row %d: %d does not fit the INT column the engine declared", col, i, n)
			}
			vals[i] = int32(n)
		}
		out.I32Val = &cliservice.TI32Column{Values: vals, Nulls: packNulls(nulls)}
	case cliservice.TTypeId_BIGINT_TYPE:
		vals := make([]int64, len(grid))
		for i, row := range grid {
			if col >= len(row) || row[col] == nil {
				nulls[i] = true
				continue
			}
			n, err := asInt64(row[col])
			if err != nil {
				return nil, fmt.Errorf("column %d row %d: %w", col, i, err)
			}
			vals[i] = n
		}
		out.I64Val = &cliservice.TI64Column{Values: vals, Nulls: packNulls(nulls)}
	case cliservice.TTypeId_DOUBLE_TYPE:
		vals := make([]float64, len(grid))
		for i, row := range grid {
			if col >= len(row) || row[col] == nil {
				nulls[i] = true
				continue
			}
			n, err := asFloat64(row[col])
			if err != nil {
				return nil, fmt.Errorf("column %d row %d: %w", col, i, err)
			}
			vals[i] = n
		}
		out.DoubleVal = &cliservice.TDoubleColumn{Values: vals, Nulls: packNulls(nulls)}
	default:
		vals := make([]string, len(grid))
		for i, row := range grid {
			if col >= len(row) || row[col] == nil {
				nulls[i] = true
				continue
			}
			vals[i] = stringify(row[col])
		}
		out.StringVal = &cliservice.TStringColumn{Values: vals, Nulls: packNulls(nulls)}
	}
	return out, nil
}

func asInt64(v any) (int64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Int64()
	case float64:
		return int64(n), nil
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("not an integer: %T", v)
	}
}

func asFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Float64()
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("not a float: %T", v)
	}
}

func stringify(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case json.Number:
		return s.String()
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(raw)
	}
}

func packNulls(flags []bool) []byte {
	if len(flags) == 0 {
		return []byte{}
	}
	b := make([]byte, (len(flags)+7)/8)
	for i, n := range flags {
		if n {
			b[i/8] |= 1 << (i % 8)
		}
	}
	return b
}

func (t *table) schema() *cliservice.TTableSchema {
	cols := make([]*cliservice.TColumnDesc, len(t.names))
	for i, name := range t.names {
		kind := cliservice.TTypeId_STRING_TYPE
		if i < len(t.types) {
			kind = t.types[i]
		}
		cols[i] = &cliservice.TColumnDesc{
			ColumnName: name,
			Position:   int32(i),
			TypeDesc: &cliservice.TTypeDesc{
				Types: []*cliservice.TTypeEntry{{
					PrimitiveEntry: &cliservice.TPrimitiveTypeEntry{Type: kind},
				}},
			},
		}
	}
	return &cliservice.TTableSchema{Columns: cols}
}

func stringTable(names []string, rows [][]string) *table {
	grid := make([][]any, len(rows))
	for i, row := range rows {
		vals := make([]any, len(names))
		for j := range names {
			if j < len(row) {
				vals[j] = row[j]
			}
		}
		grid[i] = vals
	}
	kinds := make([]cliservice.TTypeId, len(names))
	for i := range kinds {
		kinds[i] = cliservice.TTypeId_STRING_TYPE
	}
	tab, err := buildTable(names, kinds, grid)
	if err != nil {
		return emptyTable()
	}
	return tab
}

func (t *table) rowCount() int {
	if t == nil {
		return 0
	}
	for _, c := range t.cols {
		if c == nil {
			continue
		}
		switch {
		case c.StringVal != nil:
			return len(c.StringVal.Values)
		case c.BoolVal != nil:
			return len(c.BoolVal.Values)
		case c.I32Val != nil:
			return len(c.I32Val.Values)
		case c.I64Val != nil:
			return len(c.I64Val.Values)
		case c.DoubleVal != nil:
			return len(c.DoubleVal.Values)
		}
	}
	return 0
}

func (t *table) colIndex(name string) int {
	if t == nil {
		return -1
	}
	for i, n := range t.names {
		if strings.EqualFold(n, name) {
			return i
		}
	}
	return -1
}

func (t *table) namedCell(row int, names ...string) string {
	for _, name := range names {
		if i := t.colIndex(name); i >= 0 {
			return t.cell(row, i)
		}
	}
	return ""
}

func (t *table) cell(row, col int) string {
	if t == nil || col < 0 || col >= len(t.cols) || t.cols[col] == nil {
		return ""
	}
	c := t.cols[col]
	switch {
	case c.StringVal != nil && row < len(c.StringVal.Values):
		if nullAt(c.StringVal.Nulls, row) {
			return ""
		}
		return c.StringVal.Values[row]
	case c.BoolVal != nil && row < len(c.BoolVal.Values):
		if nullAt(c.BoolVal.Nulls, row) {
			return ""
		}
		if c.BoolVal.Values[row] {
			return "true"
		}
		return "false"
	case c.I32Val != nil && row < len(c.I32Val.Values):
		if nullAt(c.I32Val.Nulls, row) {
			return ""
		}
		return strconv.FormatInt(int64(c.I32Val.Values[row]), 10)
	case c.I64Val != nil && row < len(c.I64Val.Values):
		if nullAt(c.I64Val.Nulls, row) {
			return ""
		}
		return strconv.FormatInt(c.I64Val.Values[row], 10)
	case c.DoubleVal != nil && row < len(c.DoubleVal.Values):
		if nullAt(c.DoubleVal.Nulls, row) {
			return ""
		}
		return strconv.FormatFloat(c.DoubleVal.Values[row], 'f', -1, 64)
	default:
		return ""
	}
}

func nullAt(bits []byte, i int) bool {
	if i/8 >= len(bits) {
		return false
	}
	return bits[i/8]&(1<<(i%8)) != 0
}

func (t *table) rowSet() *cliservice.TRowSet {
	count := int32(len(t.names))
	return &cliservice.TRowSet{
		StartRowOffset: 0,
		Rows:           []*cliservice.TRow{},
		Columns:        t.cols,
		ColumnCount:    &count,
	}
}
