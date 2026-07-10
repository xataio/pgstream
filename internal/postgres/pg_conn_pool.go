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

type PoolOption func(*pgxpool.Config)

const maxConns = 50

func NewConnPool(ctx context.Context, url string, opts ...PoolOption) (*Pool, error) {
	escapedURL, err := escapeConnectionURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to escape connection URL: %w", err)
	}
	pgCfg, err := pgxpool.ParseConfig(escapedURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", MapError(err))
	}
	pgCfg.MaxConns = maxConns
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

// WithRawJSONDecoding makes json/jsonb column values decode to their raw text
// representation (Go string) instead of being unmarshalled into Go values
// (maps, slices, nil). Intended for read paths that need byte-faithful
// values: the default unmarshalling turns the JSON null value ('null'::jsonb)
// into Go nil, making it indistinguishable from SQL NULL, and re-marshalling
// can reorder object keys.
//
// Write paths must not use this option: it rebinds json/jsonb to a text-only
// codec, which would corrupt binary COPY encoding of those types.
func WithRawJSONDecoding() PoolOption {
	return func(cfg *pgxpool.Config) {
		prevAfterConnect := cfg.AfterConnect
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			if prevAfterConnect != nil {
				if err := prevAfterConnect(ctx, conn); err != nil {
					return err
				}
			}
			registerRawJSONDecoding(conn)
			return nil
		}
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
