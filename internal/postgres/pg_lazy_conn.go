// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"sync"
)

// AcquireFunc lazily yields a Postgres connection. Useful when a set of
// related callers want to share a single TCP connection without each one
// having to think about lifecycle.
type AcquireFunc func(ctx context.Context) (Querier, error)

// LazyConn memoises a single *Conn (or its dial error) for a URL. Cheap to
// construct: nothing is opened until Acquire is called for the first time.
// Safe for concurrent use: a caller that a deadline cut off can still be
// running when the next caller acquires the connection.
type LazyConn struct {
	url  string
	opts []ConnOption
	mu   sync.Mutex
	conn *Conn
	err  error
}

// NewLazyConn returns a LazyConn that will open a connection to url on first
// Acquire. The connection options are stored and applied on that first dial,
// not at construction.
func NewLazyConn(url string, opts ...ConnOption) *LazyConn {
	return &LazyConn{url: url, opts: opts}
}

// Acquire returns the cached conn, opening it on the first call. A dial
// failure is cached too — subsequent calls return the same error without
// retrying. A cached conn that has since been closed is replaced: pgx tears a
// connection down when a query on it is interrupted, so a caller that was cut
// off mid-query can otherwise leave the memoised conn dead for everyone after
// it.
func (l *LazyConn) Acquire(ctx context.Context) (Querier, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	if l.conn != nil && !l.conn.IsClosed() {
		return l.conn, nil
	}
	conn, err := NewConn(ctx, l.url, l.opts...)
	if err != nil {
		l.conn, l.err = nil, err
		return nil, err
	}
	l.conn = conn
	return conn, nil
}

// Close releases the underlying connection if one was opened.
func (l *LazyConn) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return nil
	}
	c := l.conn
	l.conn = nil
	return c.Close(ctx)
}
