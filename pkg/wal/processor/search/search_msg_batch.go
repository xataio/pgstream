// SPDX-License-Identifier: Apache-2.0

package search

import (
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type msgBatch struct {
	msgs       []*msg
	positions  []wal.CommitPosition
	totalBytes int
}

type msg struct {
	write        *Document
	truncate     *truncateItem
	schemaChange *schemalog.LogEntry
	bytesSize    int
	pos          wal.CommitPosition
}

type truncateItem struct {
	schemaName string
	tableID    string
}

func (m *msg) size() int {
	return m.bytesSize
}

func (m *msg) isSchemaChange() bool {
	return m.schemaChange != nil
}

func (m *msgBatch) add(msg *msg) {
	if msg == nil ||
		(msg.write == nil && msg.schemaChange == nil && msg.truncate == nil) {
		return
	}

	m.totalBytes += msg.size()
	m.msgs = append(m.msgs, msg)
	m.positions = append(m.positions, msg.pos)
}

func (m *msgBatch) drain() *msgBatch {
	batch := &msgBatch{
		msgs:       m.msgs,
		positions:  m.positions,
		totalBytes: m.totalBytes,
	}
	m.msgs = []*msg{}
	m.positions = []wal.CommitPosition{}
	m.totalBytes = 0
	return batch
}

func (m *msgBatch) size() int {
	return len(m.msgs)
}
