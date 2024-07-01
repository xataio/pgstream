// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Conn struct {
	conn *pgx.Conn
}

type Row interface {
	pgx.Row
}

func NewConn(ctx context.Context, url string) (*Conn, error) {
	pgCfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", mapError(err))
	}

	conn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", mapError(err))
	}

	return &Conn{conn: conn}, nil
}

func (c *Conn) QueryRow(ctx context.Context, query string, args ...any) Row {
	return c.conn.QueryRow(ctx, query, args...)
}

func (c *Conn) Close(ctx context.Context) error {
	return mapError(c.conn.Close(ctx))
}
