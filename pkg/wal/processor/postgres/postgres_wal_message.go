// SPDX-License-Identifier: Apache-2.0

package postgres

import "github.com/xataio/pgstream/pkg/wal"

// walMessage wraps a wal.Data with pre-fetched schema information, deferring
// query building to batch send time so that consecutive same-table DML events
// can be coalesced into bulk SQL statements.
type walMessage struct {
	data       *wal.Data
	schemaInfo schemaInfo
	isDDL      bool
}

// walMessageOverhead is the approximate size of the walMessage struct itself
// (3 pointers/structs + bool + padding).
const walMessageOverhead = 64

func (m *walMessage) Size() int {
	if m.IsEmpty() {
		return 0
	}

	size := walMessageOverhead

	if m.data != nil {
		size += len(m.data.Schema) + len(m.data.Table) + len(m.data.Action)
		size += len(m.data.LSN) + len(m.data.Timestamp)

		for _, col := range m.data.Columns {
			size += len(col.Name) + len(col.Type) + len(col.ID)
			size += interfaceOverhead + estimateArgSize(col.Value)
		}
		for _, col := range m.data.Identity {
			size += len(col.Name) + len(col.Type) + len(col.ID)
			size += interfaceOverhead + estimateArgSize(col.Value)
		}
	}

	return size
}

func (m *walMessage) IsEmpty() bool {
	return m == nil || m.data == nil
}
