// SPDX-License-Identifier: Apache-2.0

package postgres

type query struct {
	sql  string
	args []any
}

// size returns the size of the message sql query (does not include the
// parameters)
func (m *query) Size() int {
	return len(m.sql)
}

func (m *query) IsEmpty() bool {
	return m == nil || m.sql == ""
}
