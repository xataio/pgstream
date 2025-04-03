// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type BatchSender[T batch.Message] struct {
	SendMessageFn func(context.Context, *batch.WALMessage[T]) error
	CloseFn       func()
	msgChan       chan *batch.WALMessage[T]
}

func NewBatchSender[T batch.Message]() *BatchSender[T] {
	return &BatchSender[T]{
		msgChan: make(chan *batch.WALMessage[T]),
	}
}

func (m *BatchSender[T]) SendMessage(ctx context.Context, msg *batch.WALMessage[T]) error {
	if m.SendMessageFn != nil {
		return m.SendMessageFn(ctx, msg)
	}

	m.msgChan <- msg
	return nil
}

func (m *BatchSender[T]) Close() {
	close(m.msgChan)
	if m.CloseFn != nil {
		m.CloseFn()
	}
}

func (m *BatchSender[T]) GetWALMessages() []*batch.WALMessage[T] {
	msgs := []*batch.WALMessage[T]{}
	for msg := range m.msgChan {
		msgs = append(msgs, msg)
	}
	return msgs
}
