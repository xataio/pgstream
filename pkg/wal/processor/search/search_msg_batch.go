// SPDX-License-Identifier: Apache-2.0

package search

import (
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type msg struct {
	item *queueItem
	pos  wal.CommitPosition
}

type msgBatch struct {
	items      []*queueItem
	positions  []wal.CommitPosition
	totalBytes int
}

type queueItem struct {
	write        *Document
	truncate     *truncateItem
	schemaChange *schemalog.LogEntry
	bytesSize    int
}

type truncateItem struct {
	schemaName string
	tableID    string
}

func newMsg(item *queueItem, pos wal.CommitPosition) *msg {
	return &msg{
		item: item,
		pos:  pos,
	}
}

func (m *msg) size() int {
	return m.item.bytesSize
}

func (m *msg) isSchemaChange() bool {
	return m.item != nil && m.item.schemaChange != nil
}

func (m *msgBatch) add(msg *msg) {
	if msg == nil || msg.item == nil ||
		(msg.item.write == nil && msg.item.schemaChange == nil && msg.item.truncate == nil) {
		return
	}

	m.totalBytes += msg.size()
	m.items = append(m.items, msg.item)
	m.positions = append(m.positions, msg.pos)
}

func (m *msgBatch) drain() *msgBatch {
	batch := &msgBatch{
		items:      m.items,
		positions:  m.positions,
		totalBytes: m.totalBytes,
	}
	m.items = []*queueItem{}
	m.positions = []wal.CommitPosition{}
	m.totalBytes = 0
	return batch
}

func (m *msgBatch) size() int {
	return len(m.items)
}
