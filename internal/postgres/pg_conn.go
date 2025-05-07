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
	row := c.conn.QueryRow(ctx, query, args...)
	return &mappedRow{inner: row}
}

func (c *Conn) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	return rows, mapError(err)
}

func (c *Conn) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := c.conn.Exec(ctx, query, args...)
	return CommandTag{tag}, mapError(err)
}

func (c *Conn) ExecInTx(ctx context.Context, fn func(Tx) error) error {
	return c.ExecInTxWithOptions(ctx, fn, TxOptions{})
}

func (c *Conn) ExecInTxWithOptions(ctx context.Context, fn func(Tx) error, opts TxOptions) error {
	tx, err := c.conn.BeginTx(ctx, toTxOptions(opts))
	if err != nil {
		return mapError(err)
	}

	if err := fn(&Txn{Tx: tx}); err != nil {
		tx.Rollback(ctx)
		return mapError(err)
	}

	return tx.Commit(ctx)
}

func (c *Conn) Ping(ctx context.Context) error {
	return mapError(c.conn.Ping(ctx))
}

func (c *Conn) Close(ctx context.Context) error {
	return mapError(c.conn.Close(ctx))
}
