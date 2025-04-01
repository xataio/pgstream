// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
)

func TestSnapshotGenerator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testDump := []byte("test dump")
	testSchema := "test_schema"
	testTable := "test_table"
	errTest := errors.New("oh noes")

	tests := []struct {
		name           string
		snapshot       *snapshot.Snapshot
		conn           pglib.Querier
		connBuilder    pglib.QuerierBuilder
		pgdumpFn       pgdumpFn
		pgrestoreFn    pgrestoreFn
		schemalogStore schemalog.Store

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
					Schemas:          []string{pglib.QuoteIdentifier(testSchema)},
					Tables:           []string{pglib.QuoteIdentifier(testSchema) + "." + pglib.QuoteIdentifier(testTable)},
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
			name: "ok - no tables in public schema",
			snapshot: &snapshot.Snapshot{
				SchemaName: publicSchema,
				TableNames: []string{},
			},
			conn: &mocks.Querier{
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errors.New("ExecFn: should not be called")
				},
			},
			pgdumpFn: func(po pglib.PGDumpOptions) ([]byte, error) {
				return nil, errors.New("pgdumpFn: should not be called")
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
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
			name: "error - performing pgrestore output critical error",
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
				return "", pglib.NewPGRestoreErrors(errTest)
			},

			wantErr: pglib.NewPGRestoreErrors(errTest),
		},
		{
			name: "error - performing pgrestore output ignored errors",
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
				return "", pglib.NewPGRestoreErrors(&pglib.ErrRelationAlreadyExists{})
			},

			wantErr: nil,
		},
		{
			name: "error - getting target conn",
			snapshot: &snapshot.Snapshot{
				SchemaName: testSchema,
				TableNames: []string{testTable},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return nil, errTest
			},
			pgdumpFn: func(po pglib.PGDumpOptions) ([]byte, error) {
				return testDump, nil
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
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
		{
			name: "error - schemalog insertion fails",
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
				return testDump, nil
			},
			pgrestoreFn: func(po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, testDump, dump)
				return "", nil
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},

			wantErr: fmt.Errorf("inserting schemalog entry after schema snapshot: %w", errTest),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sg := SnapshotGenerator{
				sourceURL:      "source-url",
				targetURL:      "target-url",
				connBuilder:    func(ctx context.Context, s string) (pglib.Querier, error) { return tc.conn, nil },
				pgDumpFn:       tc.pgdumpFn,
				pgRestoreFn:    tc.pgrestoreFn,
				schemalogStore: tc.schemalogStore,
				logger:         log.NewNoopLogger(),
			}

			if tc.connBuilder != nil {
				sg.connBuilder = tc.connBuilder
			}

			err := sg.CreateSnapshot(context.Background(), tc.snapshot)
			require.Equal(t, err, tc.wantErr)
			sg.Close()
		})
	}
}

func TestSnapshotGenerator_schemalogExists(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		connBuilder pglib.QuerierBuilder

		wantExists bool
		wantErr    error
	}{
		{
			name: "ok - schemalog exists",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Equal(t, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)", query)
						require.Equal(t, []any{schemalog.SchemaName, schemalog.TableName}, args)
						return &mocks.Row{
							ScanFn: func(args ...any) error {
								require.Len(t, args, 1)
								exists, ok := args[0].(*bool)
								require.True(t, ok, fmt.Sprintf("exists, expected *bool, got %T", args[0]))
								*exists = true
								return nil
							},
						}
					},
				}, nil
			},

			wantExists: true,
			wantErr:    nil,
		},
		{
			name: "ok - schemalog does not exist",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Equal(t, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)", query)
						require.Equal(t, []any{schemalog.SchemaName, schemalog.TableName}, args)
						return &mocks.Row{
							ScanFn: func(args ...any) error {
								require.Len(t, args, 1)
								exists, ok := args[0].(*bool)
								require.True(t, ok, fmt.Sprintf("exists, expected *bool, got %T", args[0]))
								*exists = false
								return nil
							},
						}
					},
				}, nil
			},

			wantExists: false,
			wantErr:    nil,
		},
		{
			name: "error - getting source connection",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return nil, errTest
			},

			wantExists: false,
			wantErr:    errTest,
		},
		{
			name: "error - scanning",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
						require.Equal(t, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)", query)
						require.Equal(t, []any{schemalog.SchemaName, schemalog.TableName}, args)
						return &mocks.Row{
							ScanFn: func(args ...any) error {
								return errTest
							},
						}
					},
				}, nil
			},

			wantExists: false,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sg := SnapshotGenerator{
				connBuilder: tc.connBuilder,
			}

			exists, err := sg.schemalogExists(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantExists, exists)
		})
	}
}
