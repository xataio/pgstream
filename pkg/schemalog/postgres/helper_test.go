package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/xid"
	"github.com/xataio/pgstream/pkg/schemalog"
)

type mockQuerier struct {
	queryRowFn func(context.Context, string, ...any) pgx.Row
	execFn     func(context.Context, string, ...any) (pgconn.CommandTag, error)
	closeFn    func(context.Context)
}

func (m *mockQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return m.queryRowFn(ctx, sql, args...)
}

func (m *mockQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return m.execFn(ctx, sql, args...)
}

func (m *mockQuerier) Close(ctx context.Context) {
	m.closeFn(ctx)
}

type mockRow struct {
	logEntry *schemalog.LogEntry
	scanFn   func(args ...any) error
}

func (m *mockRow) Scan(args ...any) error {
	if m.scanFn != nil {
		return m.scanFn(args...)
	}

	id, ok := args[0].(*xid.ID)
	if !ok {
		return fmt.Errorf("unexpected type for xid.ID in scan: %T", args[0])
	}
	*id = m.logEntry.ID

	return nil
}
