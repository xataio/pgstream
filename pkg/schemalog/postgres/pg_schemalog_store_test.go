// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/schemalog"
)

func TestStore_FetchLast(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testID := xid.New()
	testLogEntry := &schemalog.LogEntry{
		ID: testID,
	}
	errTest := errors.New("oh noes")

	tests := []struct {
		name      string
		querier   pglib.Querier
		ackedOnly bool

		wantLogEntry *schemalog.LogEntry
		wantErr      error
	}{
		{
			name: "ok - without acked",
			querier: &pgmocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					require.Len(t, args, 1)
					require.Equal(t, args[0], testSchema)
					require.Equal(t,
						fmt.Sprintf("select id, version, schema_name, schema, created_at, acked from %q.%q where schema_name = $1  order by version desc limit 1", schemalog.SchemaName, schemalog.TableName),
						query)

					require.Len(t, dest, 6)
					id, ok := dest[0].(*xid.ID)
					require.True(t, ok, "expected first dest arg to be of type *xid.ID, got %T", dest[0])
					*id = testLogEntry.ID
					return nil
				},
			},

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "ok - with acked",
			querier: &pgmocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					require.Len(t, args, 1)
					require.Equal(t, args[0], testSchema)
					require.Equal(t,
						fmt.Sprintf("select id, version, schema_name, schema, created_at, acked from %q.%q where schema_name = $1 and acked order by version desc limit 1", schemalog.SchemaName, schemalog.TableName),
						query)
					require.Len(t, dest, 6)
					id, ok := dest[0].(*xid.ID)
					require.True(t, ok, "expected first dest arg to be of type *xid.ID, got %T", dest[0])
					*id = testLogEntry.ID
					return nil
				},
			},
			ackedOnly: true,

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "error - querying rows",
			querier: &pgmocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithQuerier(tc.querier)

			logEntry, err := s.FetchLast(context.Background(), testSchema, tc.ackedOnly)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLogEntry, logEntry)
		})
	}
}

func TestStore_Fetch(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testID := xid.New()
	testLogEntry := &schemalog.LogEntry{
		ID: testID,
	}
	testVersion := 1
	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier pglib.Querier

		wantLogEntry *schemalog.LogEntry
		wantErr      error
	}{
		{
			name: "ok",
			querier: &pgmocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					require.Len(t, args, 2)
					require.Equal(t, args[0], testSchema)
					require.Equal(t, args[1], testVersion)
					require.Equal(t,
						fmt.Sprintf("select id, version, schema_name, schema, created_at, acked from %q.%q where schema_name = $1 and version = $2", schemalog.SchemaName, schemalog.TableName),
						query)
					require.Len(t, dest, 6)
					id, ok := dest[0].(*xid.ID)
					require.True(t, ok, "expected first dest arg to be of type *xid.ID, got %T", dest[0])
					*id = testLogEntry.ID
					return nil
				},
			},

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "error - querying rows",
			querier: &pgmocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithQuerier(tc.querier)

			logEntry, err := s.Fetch(context.Background(), testSchema, testVersion)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLogEntry, logEntry)
		})
	}
}

func TestStore_Ack(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testID := xid.New()
	testLogEntry := &schemalog.LogEntry{
		ID:         testID,
		SchemaName: testSchema,
	}
	errTest := errors.New("oh noes")

	tests := []struct {
		name     string
		querier  pglib.Querier
		logEntry *schemalog.LogEntry

		wantErr error
	}{
		{
			name: "ok",
			querier: &pgmocks.Querier{
				ExecFn: func(_ context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
					require.Len(t, args, 2)
					require.Equal(t, args[0], testID.String())
					require.Equal(t, args[1], testSchema)
					require.Equal(t,
						fmt.Sprintf(`update %q.%q set acked = true where id = $1 and schema_name = $2`, schemalog.SchemaName, schemalog.TableName),
						query)
					return pglib.CommandTag{CommandTag: pgconn.NewCommandTag("1")}, nil
				},
			},
			logEntry: testLogEntry,

			wantErr: nil,
		},
		{
			name: "error - executing update query",
			querier: &pgmocks.Querier{
				ExecFn: func(_ context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{CommandTag: pgconn.NewCommandTag("")}, errTest
				},
			},
			logEntry: testLogEntry,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithQuerier(tc.querier)

			err := s.Ack(context.Background(), tc.logEntry)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_Insert(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testID := xid.New()
	testLogEntry := &schemalog.LogEntry{
		ID: testID,
	}
	errTest := errors.New("oh noes")
	initialVersion := 1

	tests := []struct {
		name    string
		querier pglib.Querier

		wantLogEntry *schemalog.LogEntry
		wantErr      error
	}{
		{
			name: "ok",
			querier: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := &pgmocks.Tx{
						QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
							if strings.HasPrefix(query, "select coalesce") {
								require.Len(t, dest, 1)
								version, ok := dest[0].(*int)
								require.True(t, ok, "expected first dest arg to be of type *int, got %T", dest[0])
								*version = initialVersion
								return nil
							}
							if strings.HasPrefix(query, "insert into") {
								require.Len(t, dest, 6)
								id, ok := dest[0].(*xid.ID)
								require.True(t, ok, "expected first dest arg to be of type *xid.ID, got %T", dest[0])
								*id = testLogEntry.ID
								return nil
							}
							return fmt.Errorf("unexpected query received: %v", query)
						},
					}
					return f(mockTx)
				},
			},

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "error - querying next version",
			querier: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := &pgmocks.Tx{
						QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
							if strings.HasPrefix(query, "select coalesce") {
								return errTest
							}
							return fmt.Errorf("unexpected query received: %v", query)
						},
					}
					return f(mockTx)
				},
			},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
		{
			name: "error - inserting",
			querier: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := &pgmocks.Tx{
						QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
							if strings.HasPrefix(query, "select coalesce") {
								require.Len(t, dest, 1)
								version, ok := dest[0].(*int)
								require.True(t, ok, "expected first dest arg to be of type *int, got %T", dest[0])
								*version = initialVersion
								return nil
							}
							if strings.HasPrefix(query, "insert into") {
								return errTest
							}
							return fmt.Errorf("unexpected query received: %v", query)
						},
					}
					return f(mockTx)
				},
			},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithQuerier(tc.querier)

			logEntry, err := s.Insert(context.Background(), testSchema)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLogEntry, logEntry)
		})
	}
}

func Test_mapError(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		err     error
		wantErr error
	}{
		{
			err:     errTest,
			wantErr: errTest,
		},
		{
			err:     fmt.Errorf("another error: %w", pglib.ErrNoRows),
			wantErr: schemalog.ErrNoRows,
		},
	}

	for _, tc := range tests {
		t.Run(tc.err.Error(), func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, mapError(tc.err), tc.wantErr)
		})
	}
}
