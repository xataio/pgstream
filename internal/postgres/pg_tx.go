// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

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
