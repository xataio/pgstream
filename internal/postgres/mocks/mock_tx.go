// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/internal/postgres"
)

type Tx struct {
	QueryRowFn func(ctx context.Context, query string, args ...any) postgres.Row
	QueryFn    func(ctx context.Context, query string, args ...any) (postgres.Rows, error)
	ExecFn     func(ctx context.Context, query string, args ...any) (postgres.CommandTag, error)
}

func (m *Tx) QueryRow(ctx context.Context, query string, args ...any) postgres.Row {
	return m.QueryRowFn(ctx, query, args...)
}

func (m *Tx) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	return m.QueryFn(ctx, query, args...)
}

func (m *Tx) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	return m.ExecFn(ctx, query, args...)
}
