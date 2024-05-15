// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/internal/replication"
)

type kafkaMsg struct {
	msg kafka.Message
	pos replication.LSN
}

type kafkaMsgBatch struct {
	msgs       []kafka.Message
	lastPos    replication.LSN
	totalBytes int
}

func (mb *kafkaMsgBatch) add(msg *kafkaMsg) {
	mb.msgs = append(mb.msgs, msg.msg)
	mb.totalBytes += msg.size()

	if msg.pos > mb.lastPos {
		mb.lastPos = msg.pos
	}
}

func (mb *kafkaMsgBatch) drain() *kafkaMsgBatch {
	batch := &kafkaMsgBatch{
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
func (m *kafkaMsg) size() int {
	return len(m.msg.Value)
}
