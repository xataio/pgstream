// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"sync/atomic"

	"github.com/xataio/pgstream/pkg/wal"
)

type Processor struct {
	ProcessWALEventFn func(ctx context.Context, walEvent *wal.Event) error
	CloseFn           func() error
	processCalls      atomic.Uint64
}

func (m *Processor) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) error {
	m.processCalls.Add(1)
	return m.ProcessWALEventFn(ctx, walEvent)
}

func (m *Processor) GetProcessCalls() uint {
	return uint(m.processCalls.Load())
}

func (m *Processor) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

func (m *Processor) Name() string {
	return "mock"
}
