// SPDX-License-Identifier: Apache-2.0

package tablefinder

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"github.com/xataio/pgstream/pkg/snapshot/generator/mocks"
)

func TestSnapshotTableFinder_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	errTest := errors.New("oh noes")

	newTestSnapshot := func(tables []string) *snapshot.Snapshot {
		return &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				testSchema: tables,
			},
		}
	}

	tests := []struct {
		name      string
		conn      *pgmocks.Querier
		snapshot  *snapshot.Snapshot
		generator generator.SnapshotGenerator

		wantErr error
	}{
		{
			name:     "ok - no wildcard",
			conn:     &pgmocks.Querier{},
			snapshot: newTestSnapshot([]string{"table-a", "table-b", "table-c"}),
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot([]string{"table-a", "table-b", "table-c"}), snapshot)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - only table wildcard",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, "SELECT tablename FROM pg_tables WHERE schemaname=$1", query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 1)
							table, ok := dest[0].(*string)
							require.True(t, ok)
							*table = "table-1"
							return nil
						},
						NextFn:  func(i uint) bool { return i == 1 },
						CloseFn: func() {},
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {wildcard},
				},
			},
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot([]string{"table-1"}), snapshot)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - also with table wildcard",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, "SELECT tablename FROM pg_tables WHERE schemaname=$1", query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 1)
							table, ok := dest[0].(*string)
							require.True(t, ok)
							*table = "table-1"
							return nil
						},
						NextFn:  func(i uint) bool { return i == 1 },
						CloseFn: func() {},
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {"table-a", "table-b", wildcard},
				},
			},
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot([]string{"table-1"}), snapshot)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - schema wildcard",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pgstream')":
						require.Empty(t, args)
						return &pgmocks.Rows{
							ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = testSchema
								return nil
							},
							NextFn:  func(i uint) bool { return i == 1 },
							CloseFn: func() {},
							ErrFn:   func() error { return nil },
						}, nil
					case "SELECT tablename FROM pg_tables WHERE schemaname=$1":
						require.Len(t, args, 1)
						require.Equal(t, testSchema, args[0])
						return &pgmocks.Rows{
							ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								table, ok := dest[0].(*string)
								require.True(t, ok)
								*table = "table-1"
								return nil
							},
							NextFn:  func(i uint) bool { return i == 1 },
							CloseFn: func() {},
							ErrFn:   func() error { return nil },
						}, nil
					default:
						t.Fatalf("unexpected query: %s", query)
					}
					return nil, nil // unreachable
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					wildcard:   {wildcard},
					testSchema: {"table-a", "table-b"}, // does not matter, will be replaced
				},
			},
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot([]string{"table-1"}), snapshot)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - querying schema tables",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, "SELECT tablename FROM pg_tables WHERE schemaname=$1", query)
					require.Equal(t, []any{testSchema}, args)
					return nil, errTest
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {"table-a", "table-b", wildcard},
				},
			},
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					return errors.New("CreateSnapshotFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - scanning row",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, "SELECT tablename FROM pg_tables WHERE schemaname=$1", query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						ScanFn: func(dest ...any) error {
							return errTest
						},
						NextFn:  func(i uint) bool { return i == 1 },
						CloseFn: func() {},
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {"table-a", "table-b", wildcard},
				},
			},
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					return errors.New("CreateSnapshotFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - rows error",
			conn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, "SELECT tablename FROM pg_tables WHERE schemaname=$1", query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						NextFn:  func(i uint) bool { return i == 0 },
						CloseFn: func() {},
						ErrFn:   func() error { return errTest },
					}, nil
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {"table-a", "table-b", wildcard},
				},
			},
			generator: &mocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, snapshot *snapshot.Snapshot) error {
					return errors.New("CreateSnapshotFn: should not be called")
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tableFinder := SnapshotSchemaTableFinder{
				conn:              tc.conn,
				wrapped:           tc.generator,
				schemaDiscoveryFn: discoverAllSchemas,
				tableDiscoveryFn:  discoverAllSchemaTables,
			}
			err := tableFinder.CreateSnapshot(context.Background(), tc.snapshot)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
