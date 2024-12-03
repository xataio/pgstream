// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

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

func (t *Txn) QueryRow(ctx context.Context, query string, args ...any) Row {
	row := t.Tx.QueryRow(ctx, query, args...)
	return &mappedRow{inner: row}
}

func (t *Txn) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := t.Tx.Query(ctx, query, args...)
	return rows, mapError(err)
}

func (t *Txn) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := t.Tx.Exec(ctx, query, args...)
	return CommandTag{tag}, mapError(err)
}

func toTxOptions(opts TxOptions) pgx.TxOptions {
	return pgx.TxOptions{
		IsoLevel:   pgx.TxIsoLevel(opts.IsolationLevel),
		AccessMode: pgx.TxAccessMode(opts.AccessMode),
	}
}
