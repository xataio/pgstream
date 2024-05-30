// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/pkg/wal"
)

type msg struct {
	msg kafka.Message
	pos wal.CommitPosition
}

type msgBatch struct {
	msgs       []kafka.Message
	lastPos    wal.CommitPosition
	totalBytes int
}

func (mb *msgBatch) add(m *msg) {
	mb.msgs = append(mb.msgs, m.msg)
	mb.totalBytes += m.size()

	if m.pos.After(&mb.lastPos) {
		mb.lastPos = m.pos
	}
}

func (mb *msgBatch) drain() *msgBatch {
	batch := &msgBatch{
		msgs:       mb.msgs,
		lastPos:    mb.lastPos,
		totalBytes: mb.totalBytes,
	}

	mb.msgs = []kafka.Message{}
	mb.totalBytes = 0
	return batch
}

// size returns the size of the kafka message value (does not include headers or
// other fields)
func (m *msg) size() int {
	return len(m.msg.Value)
}
