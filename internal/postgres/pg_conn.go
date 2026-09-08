// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
)

type Conn struct {
	conn *pgx.Conn
}

// ConnOption customises the driver connection configuration before the
// connection is opened. It runs after the URL is parsed and after pgstream has
// applied its own settings, so its mutations are the last word.
//
// The option may run concurrently, because callers open connections in
// parallel, so it must be safe for concurrent use.
type ConnOption func(*pgx.ConnConfig)

// DialFunc opens a connection to an address that is already resolved.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// LookupFunc resolves a host name to the addresses to try.
type LookupFunc func(ctx context.Context, host string) ([]string, error)

// WithDialFunc dials through dial instead of through pgstream's own dialler.
// The address dial receives is the resolved address the connection will use,
// for every connection target including the fallbacks the driver derives from
// a multi-host URL or from sslmode=prefer, so it is the placement that decides
// which address is reached.
//
// A nil dial leaves the dialler unchanged.
func WithDialFunc(dial DialFunc) ConnOption {
	return func(cfg *pgx.ConnConfig) {
		if dial == nil {
			return
		}
		cfg.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			if err := applyTCPKeepalive(conn); err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed configuring keepalive on the dialled connection: %w", err)
			}
			return conn, nil
		}
	}
}

// WithLookupFunc resolves host names through lookup instead of through the
// system resolver. It applies to every connection target, fallbacks included.
//
// A nil lookup leaves the resolver unchanged.
func WithLookupFunc(lookup LookupFunc) ConnOption {
	return func(cfg *pgx.ConnConfig) {
		if lookup == nil {
			return
		}
		cfg.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			return lookup(ctx, host)
		}
	}
}

func NewConn(ctx context.Context, url string, opts ...ConnOption) (*Conn, error) {
	pgCfg, err := ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", MapError(err))
	}

	configureTCPKeepalive(pgCfg)
	for _, opt := range opts {
		opt(pgCfg)
	}

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
