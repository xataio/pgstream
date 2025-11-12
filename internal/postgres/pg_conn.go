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
	pgCfg, err := ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", MapError(err))
	}

	configureTCPKeepalive(pgCfg)

	conn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", MapError(err))
	}

	registerTypesToConnMap(ctx, conn)

	return &Conn{conn: conn}, nil
}

func (c *Conn) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	row := c.conn.QueryRow(ctx, query, args...)
	return MapError(row.Scan(dest...))
}

func (c *Conn) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	return rows, MapError(err)
}

func (c *Conn) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := c.conn.Exec(ctx, query, args...)
	return CommandTag{tag}, MapError(err)
}

func (c *Conn) ExecInTx(ctx context.Context, fn func(Tx) error) error {
	return c.ExecInTxWithOptions(ctx, fn, TxOptions{})
}

func (c *Conn) ExecInTxWithOptions(ctx context.Context, fn func(Tx) error, opts TxOptions) error {
	tx, err := c.conn.BeginTx(ctx, toTxOptions(opts))
	if err != nil {
		return MapError(err)
	}

	if err := fn(&Txn{Tx: tx}); err != nil {
		tx.Rollback(ctx)
		return MapError(err)
	}

	return tx.Commit(ctx)
}

func (c *Conn) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error) {
	identifier, err := newIdentifier(tableName)
	if err != nil {
		return -1, err
	}

	// sanitize the input, removing any added quotes. The CopyFrom will sanitize
	// them and double quotes will cause errors.
	for i, c := range columnNames {
		columnNames[i] = removeQuotes(c)
	}

	return c.conn.CopyFrom(ctx, identifier, columnNames, pgx.CopyFromRows(srcRows))
}

func (c *Conn) Ping(ctx context.Context) error {
	return MapError(c.conn.Ping(ctx))
}

func (c *Conn) Close(ctx context.Context) error {
	return MapError(c.conn.Close(ctx))
}
