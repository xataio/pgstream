// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"github.com/xataio/pgstream/pkg/wal"
)

type Batch[T Message] struct {
	messages   []T
	positions  []wal.CommitPosition
	totalBytes int
}

func NewBatch[T Message](messages []T, positions []wal.CommitPosition) *Batch[T] {
	return &Batch[T]{
		messages:  messages,
		positions: positions,
	}
}

func (b *Batch[T]) GetMessages() []T {
	return b.messages
}

func (b *Batch[T]) GetCommitPositions() []wal.CommitPosition {
	return b.positions
}

func (b *Batch[T]) add(m *WALMessage[T]) {
	if !m.message.IsEmpty() {
		b.messages = append(b.messages, m.message)
		b.totalBytes += m.message.Size()
	}

	if m.position != "" {
		b.positions = append(b.positions, m.position)
	}
}

func (b *Batch[T]) drain() *Batch[T] {
	batch := &Batch[T]{
		messages:   b.messages,
		positions:  b.positions,
		totalBytes: b.totalBytes,
	}

	b.messages = []T{}
	b.totalBytes = 0
	b.positions = []wal.CommitPosition{}
	return batch
}

func (b *Batch[T]) isEmpty() bool {
	return len(b.messages) == 0 && len(b.positions) == 0
}

func (b *Batch[T]) maxBatchBytesReached(maxBatchBytes int64, msg T) bool {
	return maxBatchBytes > 0 && b.totalBytes+msg.Size() >= int(maxBatchBytes)
}
