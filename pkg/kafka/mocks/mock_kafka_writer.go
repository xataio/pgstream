// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"sync/atomic"

	"github.com/xataio/pgstream/pkg/kafka"
)

type Writer struct {
	WriteMessagesFn func(context.Context, uint64, ...kafka.Message) error
	CloseFn         func() error
	WriteCalls      uint64
}

func (m *Writer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	atomic.AddUint64(&m.WriteCalls, 1)
	return m.WriteMessagesFn(ctx, m.GetWriteCalls(), msgs...)
}

func (m *Writer) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

func (m *Writer) GetWriteCalls() uint64 {
	return atomic.LoadUint64(&m.WriteCalls)
}
