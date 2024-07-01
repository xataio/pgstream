// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xataio/pgstream/internal/postgres"
)

type Conn struct {
	QueryRowFn func(ctx context.Context, query string, args ...any) postgres.Row
	ExecFn     func(context.Context, string, ...any) (pgconn.CommandTag, error)
	CloseFn    func(context.Context) error
}

func (m *Conn) QueryRow(ctx context.Context, query string, args ...any) postgres.Row {
	return m.QueryRowFn(ctx, query, args...)
}

func (m *Conn) Close(ctx context.Context) error {
	return m.CloseFn(ctx)
}
