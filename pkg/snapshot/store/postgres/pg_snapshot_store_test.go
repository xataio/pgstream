// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/postgres"
	postgresmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
)

func TestStore_CreateSnapshotRequest(t *testing.T) {
	t.Parallel()

	testSnapshot := snapshot.Snapshot{
		SchemaName: "test-schema",
		TableNames: []string{"test-table-1", "test-table-2"},
	}
	testSnapshotRequest := &snapshot.Request{
		Snapshot: testSnapshot,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier postgres.Querier

		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					wantQuery := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, created_at, updated_at, status)
	VALUES($1, $2, now(), now(),'requested')`, snapshotsTable())
					require.Equal(t, wantQuery, s)
					wantAttr := []any{testSnapshot.SchemaName, pq.StringArray(testSnapshot.TableNames)}
					require.Equal(t, wantAttr, a)
					return postgres.CommandTag{}, nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - creating snapshot",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					return postgres.CommandTag{}, errTest
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
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

	testSnapshot := snapshot.Snapshot{
		SchemaName: "test-schema",
		TableNames: []string{"test-table-1", "test-table-2"},
	}
	testSnapshotRequest := snapshot.Request{
		Snapshot: testSnapshot,
		Status:   snapshot.StatusInProgress,
	}

	errTest := errors.New("oh noes")
	testSnapshotErr := &snapshot.Errors{Snapshot: errTest}

	tests := []struct {
		name    string
		querier postgres.Querier
		req     *snapshot.Request

		wantErr error
	}{
		{
			name: "ok - update without error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, updated_at = now()
	WHERE schema_name = $3 and table_names = $4 and status != 'completed'`,
						snapshotsTable())
					require.Equal(t, wantQuery, s)
					require.Equal(t, []any{snapshot.StatusInProgress, (*snapshot.Errors)(nil), testSnapshot.SchemaName, pq.StringArray(testSnapshot.TableNames)}, a)
					return postgres.CommandTag{}, nil
				},
			},
			req: &testSnapshotRequest,

			wantErr: nil,
		},
		{
			name: "ok - update with error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, updated_at = now()
	WHERE schema_name = $3 and table_names = $4 and status != 'completed'`,
						snapshotsTable())
					require.Equal(t, wantQuery, s)
					require.Equal(t, []any{snapshot.StatusInProgress, testSnapshotErr, testSnapshot.SchemaName, pq.StringArray(testSnapshot.TableNames)}, a)
					return postgres.CommandTag{}, nil
				},
			},
			req: func() *snapshot.Request {
				s := testSnapshotRequest
				s.Errors = testSnapshotErr
				return &s
			}(),

			wantErr: nil,
		},
		{
			name: "error - updating snapshot",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					return postgres.CommandTag{}, errTest
				},
			},
			req: &testSnapshotRequest,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
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

	testSnapshot := snapshot.Snapshot{
		SchemaName: "test-schema",
		TableNames: []string{"test-table-1", "test-table-2"},
	}
	errTest := errors.New("oh noes")
	testSnapshotRequest := snapshot.Request{
		Snapshot: testSnapshot,
		Status:   snapshot.StatusInProgress,
		Errors:   &snapshot.Errors{Snapshot: errTest},
	}

	tests := []struct {
		name    string
		querier postgres.Querier

		wantRequests []*snapshot.Request
		wantErr      error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSnapshot.SchemaName}, args)
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
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSnapshot.SchemaName}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 4)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = testSnapshot.SchemaName

							tableNames, ok := dest[1].(*[]string)
							require.True(t, ok)
							*tableNames = testSnapshot.TableNames

							status, ok := dest[2].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshotRequest.Status

							errs, ok := dest[3].(**snapshot.Errors)
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
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					return nil, errTest
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSnapshot.SchemaName}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(dest ...any) error {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			requests, err := store.GetSnapshotRequestsBySchema(context.Background(), testSnapshot.SchemaName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRequests, requests)
		})
	}
}

func TestStore_GetSnapshotRequestsByStatus(t *testing.T) {
	t.Parallel()

	testSnapshot := snapshot.Snapshot{
		SchemaName: "test-schema",
		TableNames: []string{"test-table-1", "test-table-2"},
	}
	errTest := errors.New("oh noes")
	testSnapshotRequest := snapshot.Request{
		Snapshot: testSnapshot,
		Status:   snapshot.StatusInProgress,
		Errors:   &snapshot.Errors{Snapshot: errTest},
	}

	tests := []struct {
		name    string
		querier postgres.Querier

		wantRequests []*snapshot.Request
		wantErr      error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
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
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 4)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = testSnapshot.SchemaName

							tableNames, ok := dest[1].(*[]string)
							require.True(t, ok)
							*tableNames = testSnapshot.TableNames

							status, ok := dest[2].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshotRequest.Status

							errs, ok := dest[3].(**snapshot.Errors)
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
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					return nil, errTest
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(dest ...any) error {
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
		tc := tc
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
		querier postgres.Querier

		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (postgres.CommandTag, error) {
					switch i {
					case 1:
						wantQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	errors JSONB )`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 2:
						wantQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,table_names) WHERE status != 'completed'`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					default:
						return postgres.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}

					return postgres.CommandTag{}, nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - creating table",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (postgres.CommandTag, error) {
					switch i {
					case 1:
						return postgres.CommandTag{}, errTest
					default:
						return postgres.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - creating index",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (postgres.CommandTag, error) {
					switch i {
					case 1:
						return postgres.CommandTag{}, nil
					case 2:
						return postgres.CommandTag{}, errTest
					default:
						return postgres.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
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
