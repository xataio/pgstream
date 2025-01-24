// SPDX-License-Identifier: Apache-2.0

package search

import (
	"github.com/xataio/pgstream/pkg/schemalog"
)

type msg struct {
	write        *Document
	truncate     *truncateItem
	schemaChange *schemalog.LogEntry
	bytesSize    int
}

type truncateItem struct {
	schemaName string
	tableID    string
}

func (m *msg) Size() int {
	return m.bytesSize
}

func (m *msg) IsEmpty() bool {
	return m != nil && m.write == nil && m.schemaChange == nil && m.truncate == nil
}
