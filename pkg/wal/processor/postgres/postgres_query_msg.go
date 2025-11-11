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

const (
	interfaceOverhead = 16 // interface{} = 2 pointers (type + data)
	stringOverhead    = 16 // string = pointer + length
	sliceOverhead     = 24 // slice = pointer + length + capacity
	// queryStructOverhead is the approximate size of the query struct itself
	// (3 strings + 2 slices + 1 bool + padding)
	queryStructOverhead = 104
)

// size returns the approximate size of the message including the SQL query and
// the parameters
func (m *query) Size() int {
	if m.IsEmpty() {
		return 0
	}

	size := queryStructOverhead
	size += len(m.sql) + len(m.schema) + len(m.table)

	// add the string overhead for each column name
	size += len(m.columnNames) * stringOverhead
	for _, name := range m.columnNames {
		size += len(name)
	}

	// add the interface overhead for each arg
	size += len(m.args) * interfaceOverhead
	for _, arg := range m.args {
		size += estimateArgSize(arg)
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

// Add approximate size of args to prevent memory accounting bugs. It does not
// include the interface{} slot overhead, which is already counted in the slice
// backing array.
func estimateArgSize(arg any) int {
	switch v := arg.(type) {
	case string:
		return len(v)
	case []byte:
		return len(v) + sliceOverhead // byte slice data + slice header overhead
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		bool,
		nil:
		// Stored inline in the interface{} slot, no additional heap allocation
		return 0
	default:
		// Conservative estimate for other types (e.g. time.Time, custom types)
		return 64
	}
}
