// // SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	postgresmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

func TestStore_CreateSnapshotRequest(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	testSnapshotRequest := &snapshot.Request{
		Schema: testSchema,
		Tables: testTables,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier pglib.Querier

		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					wantQuery := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, created_at, updated_at, status, mode)
	VALUES($1, $2, now(), now(),'requested', $3)`, snapshotsTable())
					require.Equal(t, wantQuery, s)
					wantAttr := []any{testSchema, pq.StringArray(testTables), snapshot.RequestModeData}
					require.Equal(t, wantAttr, a)
					return pglib.CommandTag{}, nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - creating snapshot",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			err := store.CreateSnapshotRequest(context.Background(), testSnapshotRequest)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_UpdateSnapshotRequest(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	testSnapshotRequest := snapshot.Request{
		Schema: testSchema,
		Tables: testTables,
		Status: snapshot.StatusInProgress,
	}

	errTest := errors.New("oh noes")
	testSchemaErr := snapshot.NewSchemaErrors(testSchema, errTest)

	tests := []struct {
		name    string
		querier pglib.Querier
		req     *snapshot.Request

		wantErr error
	}{
		{
			name: "ok - update without error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, updated_at = now()
	WHERE schema_name = $3 and table_names = $4 and mode = $5 and status != 'completed'`,
						snapshotsTable())
					require.Equal(t, wantQuery, s)
					require.Equal(t, []any{snapshot.StatusInProgress, (*snapshot.SchemaErrors)(nil), testSchema, pq.StringArray(testTables), snapshot.RequestModeData}, a)
					return pglib.CommandTag{}, nil
				},
			},
			req: &testSnapshotRequest,

			wantErr: nil,
		},
		{
			name: "ok - update with error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, updated_at = now()
	WHERE schema_name = $3 and table_names = $4 and mode = $5 and status != 'completed'`,
						snapshotsTable())
					require.Equal(t, wantQuery, s)
					require.Equal(t, []any{snapshot.StatusInProgress, testSchemaErr, testSchema, pq.StringArray(testTables), snapshot.RequestModeData}, a)
					return pglib.CommandTag{}, nil
				},
			},
			req: func() *snapshot.Request {
				s := testSnapshotRequest
				s.Errors = testSchemaErr
				return &s
			}(),

			wantErr: nil,
		},
		{
			name: "error - updating snapshot",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				},
			},
			req: &testSnapshotRequest,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			err := store.UpdateSnapshotRequest(context.Background(), tc.req)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_GetSnapshotRequestsBySchema(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	errTest := errors.New("oh noes")
	testSnapshotRequest := snapshot.Request{
		Schema: testSchema,
		Tables: testTables,
		Status: snapshot.StatusInProgress,
		Mode:   snapshot.RequestModeData,
		Errors: snapshot.NewSchemaErrors(testSchema, errTest),
	}

	tests := []struct {
		name    string
		querier pglib.Querier

		wantRequests []*snapshot.Request
		wantErr      error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSchema}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(_ uint) bool { return false },
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{},
			wantErr:      nil,
		},
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSchema}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 5)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = testSchema

							tableNames, ok := dest[1].(*[]string)
							require.True(t, ok)
							*tableNames = testTables

							status, ok := dest[2].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshotRequest.Status

							mode, ok := dest[3].(*snapshot.RequestMode)
							require.True(t, ok)
							*mode = testSnapshotRequest.Mode

							errs, ok := dest[4].(**snapshot.SchemaErrors)
							require.True(t, ok)
							*errs = testSnapshotRequest.Errors
							return nil
						},
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{
				&testSnapshotRequest,
			},
			wantErr: nil,
		},
		{
			name: "error - querying",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSchema}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
					}, nil
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			requests, err := store.GetSnapshotRequestsBySchema(context.Background(), testSchema)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRequests, requests)
		})
	}
}

func TestStore_GetSnapshotRequestsByStatus(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	errTest := errors.New("oh noes")
	testSnapshotRequest := snapshot.Request{
		Schema: testSchema,
		Tables: testTables,
		Status: snapshot.StatusInProgress,
		Mode:   snapshot.RequestModeData,
		Errors: snapshot.NewSchemaErrors(testSchema, errTest),
	}

	tests := []struct {
		name    string
		querier pglib.Querier

		wantRequests []*snapshot.Request
		wantErr      error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE status = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, wantQuery, query)
					require.Equal(t, []any{snapshot.StatusInProgress}, args)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(_ uint) bool { return false },
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{},
			wantErr:      nil,
		},
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE status = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, wantQuery, query)
					require.Equal(t, []any{snapshot.StatusInProgress}, args)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 5)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = testSchema

							tableNames, ok := dest[1].(*[]string)
							require.True(t, ok)
							*tableNames = testTables

							status, ok := dest[2].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshotRequest.Status

							mode, ok := dest[3].(*snapshot.RequestMode)
							require.True(t, ok)
							*mode = testSnapshotRequest.Mode

							errs, ok := dest[4].(**snapshot.SchemaErrors)
							require.True(t, ok)
							*errs = testSnapshotRequest.Errors
							return nil
						},
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{
				&testSnapshotRequest,
			},
			wantErr: nil,
		},
		{
			name: "error - querying",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE status = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, wantQuery, query)
					require.Equal(t, []any{snapshot.StatusInProgress}, args)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
					}, nil
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			requests, err := store.GetSnapshotRequestsByStatus(context.Background(), snapshot.StatusInProgress)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRequests, requests)
		})
	}
}

func TestStore_createTable(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier pglib.Querier

		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						wantQuery := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, store.SchemaName)
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 2:
						wantQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	mode TEXT NOT NULL DEFAULT 'data' CHECK (mode IN ('data', 'schema-only')),
	errors JSONB )`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 3:
						wantQuery := fmt.Sprintf(`ALTER TABLE %s
	ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'data' CHECK (mode IN ('data', 'schema-only'))`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 4:
						wantQuery := fmt.Sprintf(`DROP INDEX IF EXISTS %s.schema_table_status_unique_index`, store.SchemaName)
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 5:
						wantQuery := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.snapshot_table_names_hash(names TEXT[])
	RETURNS TEXT LANGUAGE sql IMMUTABLE AS $func$ SELECT md5(array_to_string(names, ',')) $func$`, store.SchemaName)
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 6:
						wantQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,%s.snapshot_table_names_hash(table_names)) WHERE status != 'completed'`, snapshotsTable(), store.SchemaName)
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}

					return pglib.CommandTag{}, nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - creating schema",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - creating table",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, nil
					case 2:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - adding mode column",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2:
						return pglib.CommandTag{}, nil
					case 3:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - dropping legacy index",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2, 3:
						return pglib.CommandTag{}, nil
					case 4:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - creating hash function",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2, 3, 4:
						return pglib.CommandTag{}, nil
					case 5:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - creating index",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2, 3, 4, 5:
						return pglib.CommandTag{}, nil
					case 6:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := Store{
				conn: tc.querier,
			}
			err := store.createTable(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
