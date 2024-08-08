// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/wal"
)

type msg struct {
	msg kafka.Message
	pos wal.CommitPosition
}

type msgBatch struct {
	msgs       []kafka.Message
	positions  []wal.CommitPosition
	totalBytes int
}

func (mb *msgBatch) add(m *msg) {
	if m.msg.Value != nil {
		mb.msgs = append(mb.msgs, m.msg)
		mb.totalBytes += m.size()
	}

	if m.pos != "" {
		mb.positions = append(mb.positions, m.pos)
	}
}

func (mb *msgBatch) drain() *msgBatch {
	batch := &msgBatch{
		msgs:       mb.msgs,
		positions:  mb.positions,
		totalBytes: mb.totalBytes,
	}

	mb.msgs = []kafka.Message{}
	mb.totalBytes = 0
	mb.positions = []wal.CommitPosition{}
	return batch
}

func (mb *msgBatch) isEmpty() bool {
	return len(mb.msgs) == 0 && len(mb.positions) == 0
}

// size returns the size of the kafka message value (does not include headers or
// other fields)
func (m *msg) size() int {
	return len(m.msg.Value)
}

func (m *msg) isKeepAlive() bool {
	return m.msg.Value == nil && m.pos != ""
}
