// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Querier interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, dest []any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	ExecInTx(ctx context.Context, fn func(tx Tx) error) error
	ExecInTxWithOptions(ctx context.Context, fn func(tx Tx) error, txOpts TxOptions) error
	CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// StatementCacheResetter is implemented by a Querier that keeps prepared
// statements, so that a caller can drop them after a schema change.
type StatementCacheResetter interface {
	ResetStatementCache()
}

// ResetStatementCache drops the prepared statements the querier keeps, when it
// keeps any.
//
// pgx prepares a statement once for each connection and remembers the
// parameter types it learned. After `ALTER TABLE ... ALTER COLUMN ... TYPE
// ...` those types are wrong, and the next write with a value the old type
// cannot hold fails in the client, before it reaches postgres:
//
//	failed to encode args[1] for int4 (OID 23)
//
// The writer then drops that row and moves the checkpoint past it, so the
// replica loses it for good.
func ResetStatementCache(q Querier) {
	if resetter, ok := q.(StatementCacheResetter); ok {
		resetter.ResetStatementCache()
	}
}

type Row interface {
	pgx.Row
}

type Rows interface {
	pgx.Rows
}

type CommandTag struct {
	pgconn.CommandTag
}
