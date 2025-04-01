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
			SchemaName: testSchema,
			TableNames: tables,
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
			name: "ok - only wildcard",
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
				SchemaName: testSchema,
				TableNames: []string{wildcard},
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
			name: "ok - with wildcard",
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
				SchemaName: testSchema,
				TableNames: []string{"table-a", "table-b", wildcard},
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
				SchemaName: testSchema,
				TableNames: []string{"table-a", "table-b", wildcard},
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
				SchemaName: testSchema,
				TableNames: []string{"table-a", "table-b", wildcard},
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
				SchemaName: testSchema,
				TableNames: []string{"table-a", "table-b", wildcard},
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

			tableFinder := SnapshotTableFinder{
				conn:    tc.conn,
				wrapped: tc.generator,
			}
			err := tableFinder.CreateSnapshot(context.Background(), tc.snapshot)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
