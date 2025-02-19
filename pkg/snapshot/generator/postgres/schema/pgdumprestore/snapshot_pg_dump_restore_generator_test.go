// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
)

func TestSnapshotGenerator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testDump := []byte("test dump")
	testSchema := "test_schema"
	testTable := "test_table"
	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		snapshot    *snapshot.Snapshot
		conn        pglib.Querier
		pgdumpFn    pgdumpFn
		pgrestoreFn pgrestoreFn

		wantErr error
	}{
		{
			name: "ok",
			snapshot: &snapshot.Snapshot{
				SchemaName: testSchema,
				TableNames: []string{testTable},
			},
			conn: &mocks.Querier{
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					require.Equal(t, "CREATE SCHEMA IF NOT EXISTS "+testSchema, query)
					return pglib.CommandTag{}, nil
				},
			},
			pgdumpFn: func(po pglib.PGDumpOptions) ([]byte, error) {
				require.Equal(t, pglib.PGDumpOptions{
					ConnectionString: "source-url",
					Format:           "c",
					SchemaOnly:       true,
					Schemas:          []string{testSchema},
					Tables:           []string{testSchema + "." + testTable},
				}, po)
				return testDump, nil
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					SchemaOnly:       true,
				}, po)
				require.Equal(t, testDump, dump)
				return "", nil
			},

			wantErr: nil,
		},
		{
			name: "error - performing pgdump",
			snapshot: &snapshot.Snapshot{
				SchemaName: testSchema,
				TableNames: []string{testTable},
			},
			conn: &mocks.Querier{
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errors.New("ExecFn: should not be called")
				},
			},
			pgdumpFn: func(po pglib.PGDumpOptions) ([]byte, error) {
				return nil, errTest
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - performing pgrestore",
			snapshot: &snapshot.Snapshot{
				SchemaName: publicSchema,
				TableNames: []string{testTable},
			},
			conn: &mocks.Querier{
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errors.New("ExecFn: should not be called")
				},
			},
			pgdumpFn: func(po pglib.PGDumpOptions) ([]byte, error) {
				return testDump, nil
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errTest
			},

			wantErr: errTest,
		},
		{
			name: "error - creating schema",
			snapshot: &snapshot.Snapshot{
				SchemaName: testSchema,
				TableNames: []string{testTable},
			},
			conn: &mocks.Querier{
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				},
			},
			pgdumpFn: func(po pglib.PGDumpOptions) ([]byte, error) {
				return testDump, nil
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sg := SnapshotGenerator{
				sourceURL:   "source-url",
				targetURL:   "target-url",
				targetConn:  tc.conn,
				pgDumpFn:    tc.pgdumpFn,
				pgRestoreFn: tc.pgrestoreFn,
				logger:      log.NewNoopLogger(),
			}

			err := sg.CreateSnapshot(context.Background(), tc.snapshot)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
