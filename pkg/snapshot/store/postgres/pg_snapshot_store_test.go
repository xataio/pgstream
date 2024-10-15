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

	testSnapshot := &snapshot.Snapshot{
		SchemaName:          "test-schema",
		TableName:           "test-table",
		IdentityColumnNames: []string{"id"},
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
					wantQuery := fmt.Sprintf(`INSERT INTO %s (schema_name, table_name, identity_column_names, created_at, updated_at, status)
	VALUES($1, $2, $3,'now()','now()','requested')`, snapshotsTable())
					require.Equal(t, wantQuery, s)
					wantAttr := []any{testSnapshot.SchemaName, testSnapshot.TableName, pq.StringArray(testSnapshot.IdentityColumnNames)}
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
			err := store.CreateSnapshotRequest(context.Background(), testSnapshot)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_UpdateSnapshotRequest(t *testing.T) {
	t.Parallel()

	testSnapshot := snapshot.Snapshot{
		SchemaName:          "test-schema",
		TableName:           "test-table",
		IdentityColumnNames: []string{"id"},
		Status:              snapshot.StatusInProgress,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier postgres.Querier
		req     *snapshot.Snapshot

		wantErr error
	}{
		{
			name: "ok - update without error string",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = '%s', error = '%s', updated_at = 'now()'
	WHERE schema_name = '%s' and table_name = '%s' and status != 'completed'`,
						snapshotsTable(), snapshot.StatusInProgress, "", testSnapshot.SchemaName, testSnapshot.TableName)
					require.Equal(t, wantQuery, s)
					require.Empty(t, a)
					return postgres.CommandTag{}, nil
				},
			},
			req: &testSnapshot,

			wantErr: nil,
		},
		{
			name: "ok - update with error string",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (postgres.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = '%s', error = '%s', updated_at = 'now()'
	WHERE schema_name = '%s' and table_name = '%s' and status != 'completed'`,
						snapshotsTable(), snapshot.StatusInProgress, errTest.Error(), testSnapshot.SchemaName, testSnapshot.TableName)
					require.Equal(t, wantQuery, s)
					require.Empty(t, a)
					return postgres.CommandTag{}, nil
				},
			},
			req: func() *snapshot.Snapshot {
				s := testSnapshot
				s.Error = errTest
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
			req: &testSnapshot,

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

func TestStore_GetSnapshotRequests(t *testing.T) {
	t.Parallel()

	testSnapshot := &snapshot.Snapshot{
		SchemaName:          "test-schema",
		TableName:           "test-table",
		IdentityColumnNames: []string{"id"},
		Status:              snapshot.StatusInProgress,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier postgres.Querier

		wantSnapshots []*snapshot.Snapshot
		wantErr       error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_name,identity_column_names,status FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(_ uint) bool { return false },
					}, nil
				},
			},

			wantSnapshots: []*snapshot.Snapshot{},
			wantErr:       nil,
		},
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_name,identity_column_names,status FROM %s
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

							tableName, ok := dest[1].(*string)
							require.True(t, ok)
							*tableName = testSnapshot.TableName

							idColumns, ok := dest[2].(*[]string)
							require.True(t, ok)
							*idColumns = testSnapshot.IdentityColumnNames

							status, ok := dest[3].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshot.Status
							return nil
						},
					}, nil
				},
			},

			wantSnapshots: []*snapshot.Snapshot{
				testSnapshot,
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

			wantSnapshots: nil,
			wantErr:       errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,table_name,identity_column_names,status FROM %s
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

			wantSnapshots: nil,
			wantErr:       errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			snapshots, err := store.GetSnapshotRequests(context.Background(), snapshot.StatusInProgress)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantSnapshots, snapshots)
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
	table_name TEXT,
	identity_column_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	error TEXT )`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 2:
						wantQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,table_name) WHERE status != 'completed'`, snapshotsTable())
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
