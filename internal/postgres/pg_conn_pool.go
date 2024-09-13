// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	*pgxpool.Pool
}

func NewConnPool(ctx context.Context, url string) (*Pool, error) {
	pgCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", mapError(err))
	}

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

func (c *Pool) Close(_ context.Context) error {
	c.Pool.Close()
	return nil
}
