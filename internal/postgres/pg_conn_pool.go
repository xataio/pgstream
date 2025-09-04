// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	*pgxpool.Pool
}

const maxConns = 50

func NewConnPool(ctx context.Context, url string) (*Pool, error) {
	pgCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", mapError(err))
	}
	pgCfg.MaxConns = maxConns
	pgCfg.AfterConnect = registerTypesToConnMap

	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create a postgres connection pool: %w", mapError(err))
	}

	return &Pool{Pool: pool}, nil
}

func (c *Pool) QueryRow(ctx context.Context, query string, args ...any) Row {
	row := c.Pool.QueryRow(ctx, query, args...)
	return &mappedRow{inner: row}
}

func (c *Pool) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.Pool.Query(ctx, query, args...)
	return rows, mapError(err)
}

func (c *Pool) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := c.Pool.Exec(ctx, query, args...)
	return CommandTag{tag}, mapError(err)
}

func (c *Pool) ExecInTx(ctx context.Context, fn func(Tx) error) error {
	return c.ExecInTxWithOptions(ctx, fn, TxOptions{})
}

func (c *Pool) ExecInTxWithOptions(ctx context.Context, fn func(Tx) error, opts TxOptions) error {
	tx, err := c.BeginTx(ctx, toTxOptions(opts))
	if err != nil {
		return mapError(err)
	}

	if err := fn(&Txn{Tx: tx}); err != nil {
		tx.Rollback(ctx)
		return mapError(err)
	}

	return tx.Commit(ctx)
}

func (c *Pool) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error) {
	identifier, err := newIdentifier(tableName)
	if err != nil {
		return -1, err
	}

	// sanitize the input, removing any added quotes. The CopyFrom will sanitize
	// them and double quotes will cause errors.
	for i, c := range columnNames {
		columnNames[i] = removeQuotes(c)
	}

	return c.Pool.CopyFrom(ctx, identifier, columnNames, pgx.CopyFromRows(srcRows))
}

func (c *Pool) Ping(ctx context.Context) error {
	return mapError(c.Pool.Ping(ctx))
}

func (c *Pool) Close(_ context.Context) error {
	c.Pool.Close()
	return nil
}
