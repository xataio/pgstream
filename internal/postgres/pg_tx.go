// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Tx interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, dest []any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
}

type TxIsolationLevel string

const (
	Serializable    TxIsolationLevel = "serializable"
	RepeatableRead  TxIsolationLevel = "repeatable read"
	ReadCommitted   TxIsolationLevel = "read committed"
	ReadUncommitted TxIsolationLevel = "read uncommitted"
)

type TxAccessMode string

const (
	ReadWrite TxAccessMode = "read write"
	ReadOnly  TxAccessMode = "read only"
)

type TxOptions struct {
	IsolationLevel TxIsolationLevel
	AccessMode     TxAccessMode
}

type Txn struct {
	pgx.Tx
}

func (t *Txn) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	row := t.Tx.QueryRow(ctx, query, args...)
	return MapError(row.Scan(dest...))
}

func (t *Txn) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := t.Tx.Query(ctx, query, args...)
	return rows, MapError(err)
}

func (t *Txn) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := t.Tx.Exec(ctx, query, args...)
	return CommandTag{tag}, MapError(err)
}

func (t *Txn) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error) {
	identifier, err := newIdentifier(tableName)
	if err != nil {
		return -1, err
	}

	// sanitize the input, removing any added quotes. The CopyFrom will sanitize
	// them and double quotes will cause errors.
	for i, c := range columnNames {
		columnNames[i] = removeQuotes(c)
	}

	return t.Tx.CopyFrom(ctx, identifier, columnNames, pgx.CopyFromRows(srcRows))
}

func toTxOptions(opts TxOptions) pgx.TxOptions {
	return pgx.TxOptions{
		IsoLevel:   pgx.TxIsoLevel(opts.IsolationLevel),
		AccessMode: pgx.TxAccessMode(opts.AccessMode),
	}
}
