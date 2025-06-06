// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type RowsProcessor struct {
	ProcessRowFn func(context.Context, *snapshot.Row) error
	CloseFn      func() error
}

func (m *RowsProcessor) ProcessRow(ctx context.Context, row *snapshot.Row) error {
	return m.ProcessRowFn(ctx, row)
}

func (m *RowsProcessor) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
