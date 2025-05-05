// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Querier interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	ExecInTx(ctx context.Context, fn func(tx Tx) error) error
	ExecInTxWithOptions(ctx context.Context, fn func(tx Tx) error, txOpts TxOptions) error
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

type Row interface {
	pgx.Row
}

type Rows interface {
	pgx.Rows
}

type Tx interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
}

type CommandTag struct {
	pgconn.CommandTag
}

type mappedRow struct {
	inner Row
}

func (mr *mappedRow) Scan(dest ...any) error {
	err := mr.inner.Scan(dest...)
	return mapError(err)
}
