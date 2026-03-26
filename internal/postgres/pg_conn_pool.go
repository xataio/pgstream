// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	*pgxpool.Pool
}

type PoolOption func(*pgxpool.Config)

const defaultMaxConns = 50

// getMaxConns returns the pool size from PGSTREAM_POOL_MAX_CONNECTIONS env var,
// falling back to defaultMaxConns. This allows the deployer to cap all
// connection pools globally to avoid exhausting the source database's
// max_connections — pgstream creates multiple pools to the same source, and
// the default of 50 per pool can overwhelm small databases (25-100 connections).
func getMaxConns() int32 {
	if v := os.Getenv("PGSTREAM_POOL_MAX_CONNECTIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return int32(n)
		}
	}
	return defaultMaxConns
}

func NewConnPool(ctx context.Context, url string, opts ...PoolOption) (*Pool, error) {
	escapedURL, err := escapeConnectionURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to escape connection URL: %w", err)
	}
	pgCfg, err := pgxpool.ParseConfig(escapedURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", MapError(err))
	}
	pgCfg.MaxConns = getMaxConns()
	pgCfg.AfterConnect = registerTypesToConnMap

	configureTCPKeepalive(pgCfg.ConnConfig)

	for _, opt := range opts {
		opt(pgCfg)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create a postgres connection pool: %w", MapError(err))
	}

	return &Pool{Pool: pool}, nil
}

func WithMaxConnections(maxConns int32) PoolOption {
	return func(cfg *pgxpool.Config) {
		cfg.MaxConns = maxConns
	}
}

func (c *Pool) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	row := c.Pool.QueryRow(ctx, query, args...)
	return MapError(row.Scan(dest...))
}

func (c *Pool) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.Pool.Query(ctx, query, args...)
	return rows, MapError(err)
}

func (c *Pool) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := c.Pool.Exec(ctx, query, args...)
	return CommandTag{tag}, MapError(err)
}

func (c *Pool) ExecInTx(ctx context.Context, fn func(Tx) error) error {
	return c.ExecInTxWithOptions(ctx, fn, TxOptions{})
}

func (c *Pool) ExecInTxWithOptions(ctx context.Context, fn func(Tx) error, opts TxOptions) error {
	tx, err := c.BeginTx(ctx, toTxOptions(opts))
	if err != nil {
		return MapError(err)
	}

	if err := fn(&Txn{Tx: tx}); err != nil {
		tx.Rollback(ctx)
		return MapError(err)
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
	return MapError(c.Pool.Ping(ctx))
}

func (c *Pool) Close(_ context.Context) error {
	c.Pool.Close()
	return nil
}
