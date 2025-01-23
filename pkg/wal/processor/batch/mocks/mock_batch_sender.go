// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type BatchSender[T batch.Message] struct {
	AddToBatchFn func(context.Context, *batch.WALMessage[T]) error
	SendFn       func(context.Context) error
	msgChan      chan *batch.WALMessage[T]
}

func NewBatchSender[T batch.Message]() *BatchSender[T] {
	return &BatchSender[T]{
		msgChan: make(chan *batch.WALMessage[T]),
	}
}

func (m *BatchSender[T]) AddToBatch(ctx context.Context, msg *batch.WALMessage[T]) error {
	if m.AddToBatchFn != nil {
		return m.AddToBatchFn(ctx, msg)
	}

	m.msgChan <- msg
	return nil
}

func (m *BatchSender[T]) Close() {
	close(m.msgChan)
}

func (m *BatchSender[T]) Send(ctx context.Context) error {
	if m.SendFn != nil {
		return m.SendFn(ctx)
	}

	_ = m.GetWALMessages()
	return nil
}

func (m *BatchSender[T]) GetWALMessages() []*batch.WALMessage[T] {
	msgs := []*batch.WALMessage[T]{}
	for msg := range m.msgChan {
		msgs = append(msgs, msg)
	}
	return msgs
}
