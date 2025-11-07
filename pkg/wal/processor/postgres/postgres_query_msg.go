// SPDX-License-Identifier: Apache-2.0

package postgres

type query struct {
	schema      string
	table       string
	sql         string
	columnNames []string
	args        []any
	isDDL       bool
}

// size returns the approximate size of the message including the SQL query and
// the parameters
func (m *query) Size() int {
	size := len(m.sql) + len(m.schema) + len(m.table)

	// Add size of column names
	for _, col := range m.columnNames {
		size += len(col) + 16 // string data + string header overhead
	}

	// Add approximate size of args to prevent memory accounting bugs.
	// Each arg includes both the value size and Go's interface/pointer overhead.
	for _, arg := range m.args {
		switch v := arg.(type) {
		case string:
			size += len(v) + 16 // string data + string header overhead
		case []byte:
			size += len(v) + 24 // byte slice data + slice header overhead
		case nil:
			size += 8 // nil interface overhead
		case int, int8, int16, int32, int64:
			size += 16 // int value + interface overhead
		case uint, uint8, uint16, uint32, uint64:
			size += 16 // uint value + interface overhead
		case float32, float64:
			size += 16 // float value + interface overhead
		case bool:
			size += 16 // bool value + interface overhead
		default:
			// Conservative estimate for other types (e.g. time.Time, custom types)
			size += 64
		}
	}

	return size
}

func (m *query) IsEmpty() bool {
	return m == nil || m.sql == ""
}

func (m *query) getSQL() string {
	if m == nil {
		return ""
	}
	return m.sql
}

func (m *query) getArgs() []any {
	if m == nil {
		return nil
	}
	return m.args
}
