// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/internal/postgres"
)

type Querier struct {
	QueryRowFn func(ctx context.Context, query string, args ...any) postgres.Row
	QueryFn    func(ctx context.Context, query string, args ...any) (postgres.Rows, error)
	ExecFn     func(context.Context, uint, string, ...any) (postgres.CommandTag, error)
	ExecInTxFn func(context.Context, func(tx postgres.Tx) error) error
	CloseFn    func(context.Context) error
	execCalls  uint
}

func (m *Querier) QueryRow(ctx context.Context, query string, args ...any) postgres.Row {
	return m.QueryRowFn(ctx, query, args...)
}

func (m *Querier) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	return m.QueryFn(ctx, query, args...)
}

func (m *Querier) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	m.execCalls++
	return m.ExecFn(ctx, m.execCalls, query, args...)
}

func (m *Querier) ExecInTx(ctx context.Context, fn func(tx postgres.Tx) error) error {
	return m.ExecInTxFn(ctx, fn)
}

func (m *Querier) Close(ctx context.Context) error {
	return m.CloseFn(ctx)
}
