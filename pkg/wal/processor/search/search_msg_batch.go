// SPDX-License-Identifier: Apache-2.0

package search

import (
	"github.com/xataio/pgstream/pkg/wal"
)

type msg struct {
	write      *Document
	truncate   *truncateItem
	schemaDiff *wal.SchemaDiff
	bytesSize  int
}

type truncateItem struct {
	schemaName string
	tableID    string
}

func (m *msg) Size() int {
	return m.bytesSize
}

func (m *msg) IsEmpty() bool {
	return m != nil && m.write == nil && m.truncate == nil && m.schemaDiff == nil
}
