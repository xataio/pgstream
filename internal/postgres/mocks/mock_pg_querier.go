// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/internal/postgres"
)

type Querier struct {
	QueryRowFn func(ctx context.Context, query string, args ...any) postgres.Row
	QueryFn    func(ctx context.Context, query string, args ...any) (postgres.Rows, error)
	ExecFn     func(context.Context, string, ...any) (postgres.CommandTag, error)
	CloseFn    func(context.Context) error
}

func (m *Querier) QueryRow(ctx context.Context, query string, args ...any) postgres.Row {
	return m.QueryRowFn(ctx, query, args...)
}

func (m *Querier) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	return m.QueryFn(ctx, query, args...)
}

func (m *Querier) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	return m.ExecFn(ctx, query, args...)
}

func (m *Querier) Close(ctx context.Context) error {
	return m.CloseFn(ctx)
}
