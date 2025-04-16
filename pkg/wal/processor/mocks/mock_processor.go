// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

type Processor struct {
	ProcessWALEventFn func(ctx context.Context, walEvent *wal.Event) error
	processCalls      uint
}

func (m *Processor) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) error {
	m.processCalls++
	return m.ProcessWALEventFn(ctx, walEvent)
}

func (m *Processor) GetProcessCalls() uint {
	return m.processCalls
}

func (m *Processor) Name() string {
	return "mock"
}
