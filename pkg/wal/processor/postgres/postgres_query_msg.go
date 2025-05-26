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

// size returns the size of the message sql query (does not include the
// parameters)
func (m *query) Size() int {
	return len(m.sql)
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
