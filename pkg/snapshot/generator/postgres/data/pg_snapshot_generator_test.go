// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/log/zerolog"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator/mocks"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestSnapshotGenerator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testTable1 := "test-table-1"
	testTable2 := "test-table-2"
	testSchema := "test-schema"
	testSnapshot := &snapshot.Snapshot{
		SchemaTables: map[string][]string{
			testSchema: {testTable1},
		},
	}

	txOptions := pglib.TxOptions{
		IsolationLevel: pglib.RepeatableRead,
		AccessMode:     pglib.ReadOnly,
	}

	testSnapshotID := "test-snapshot-id"
	testPageCount := 1
	testUUID := uuid.New().String()
	testColumns := []snapshot.Column{
		{Name: "id", Type: "uuid", Value: testUUID},
		{Name: "name", Type: "text", Value: "alice"},
	}

	testRow := func(tableName string, columns []snapshot.Column) *snapshot.Row {
		return &snapshot.Row{
			Schema:  testSchema,
			Table:   tableName,
			Columns: columns,
		}
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name          string
		querier       pglib.Querier
		snapshot      *snapshot.Snapshot
		schemaWorkers uint

		wantRows []*snapshot.Row
		wantErr  error
	}{
		{
			name: "ok",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								require.Equal(t, fmt.Sprintf("SELECT * FROM %q.%q WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'", testSchema, testTable1, 0, 10), query)
								require.Len(t, args, 0)
								return &pgmocks.Rows{
									CloseFn: func() {},
									NextFn:  func(i uint) bool { return i == 1 },
									FieldDescriptionsFn: func() []pgconn.FieldDescription {
										return []pgconn.FieldDescription{
											{Name: "id", DataTypeOID: pgtype.UUIDOID},
											{Name: "name", DataTypeOID: pgtype.TextOID},
										}
									},
									ValuesFn: func() ([]any, error) {
										return []any{testUUID, "alice"}, nil
									},
									ErrFn: func() error { return nil },
								}, nil
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr:  nil,
			wantRows: []*snapshot.Row{testRow(testTable1, testColumns)},
		},
		{
			name: "ok - multiple tables and multiple workers",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					mockTx := pgmocks.Tx{
						QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
							if query == "SELECT pg_export_snapshot()" {
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							}
							if query == "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2" {
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							}
							return &pgmocks.Row{
								ScanFn: func(args ...any) error { return fmt.Errorf("unexpected call to QueryRowFn: %s", query) },
							}
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return &pgmocks.Rows{
								CloseFn: func() {},
								NextFn:  func(i uint) bool { return i == 1 },
								FieldDescriptionsFn: func() []pgconn.FieldDescription {
									return []pgconn.FieldDescription{
										{Name: "id", DataTypeOID: pgtype.UUIDOID},
										{Name: "name", DataTypeOID: pgtype.TextOID},
									}
								},
								ValuesFn: func() ([]any, error) {
									return []any{testUUID, "alice"}, nil
								},
								ErrFn: func() error { return nil },
							}, nil
						},
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							require.Len(t, args, 0)
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable1, testTable2},
				},
			},
			schemaWorkers: 2,

			wantErr:  nil,
			wantRows: []*snapshot.Row{testRow(testTable1, testColumns), testRow(testTable2, testColumns)},
		},
		{
			name: "ok - unsupported column type",
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
					return &pgmocks.Row{
						ScanFn: func(args ...any) error { return errTest },
					}
				},
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								require.Equal(t, fmt.Sprintf("SELECT * FROM %q.%q WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'", testSchema, testTable1, 0, 10), query)
								require.Len(t, args, 0)
								return &pgmocks.Rows{
									CloseFn: func() {},
									NextFn:  func(i uint) bool { return i == 1 },
									FieldDescriptionsFn: func() []pgconn.FieldDescription {
										return []pgconn.FieldDescription{
											{Name: "id", DataTypeOID: pgtype.UUIDOID},
											{Name: "name", DataTypeOID: pgtype.TextOID},
											{Name: "unsupported", DataTypeOID: 99999},
										}
									},
									ValuesFn: func() ([]any, error) {
										return []any{testUUID, "alice", 1}, nil
									},
									ErrFn: func() error { return nil },
								}, nil
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr:  nil,
			wantRows: []*snapshot.Row{testRow(testTable1, testColumns)},
		},
		{
			name: "ok - no data",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								require.Equal(t, fmt.Sprintf("SELECT * FROM %q.%q WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'", testSchema, testTable1, 0, 10), query)
								require.Len(t, args, 0)
								return &pgmocks.Rows{
									CloseFn:             func() {},
									NextFn:              func(i uint) bool { return i == 0 },
									FieldDescriptionsFn: func() []pgconn.FieldDescription { return []pgconn.FieldDescription{} },
									ValuesFn:            func() ([]any, error) { return []any{}, nil },
									ErrFn:               func() error { return nil },
								}, nil
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr:  nil,
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - exporting snapshot",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										return errTest
									},
								}
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr:  snapshot.NewErrors(testSchema, fmt.Errorf("exporting snapshot: %w", errTest)),
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - setting transaction snapshot before table page count",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, errTest
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: fmt.Sprintf("setting transaction snapshot: %v", errTest),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - getting table page count",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										return errTest
									},
								}
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: fmt.Sprintf("getting page count for table test-schema.test-table-1: %v", errTest),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - setting transaction snapshot for table range",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, errTest
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: fmt.Sprintf("setting transaction snapshot: %v", errTest),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - querying range data",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								require.Equal(t, fmt.Sprintf("SELECT * FROM %q.%q WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'", testSchema, testTable1, 0, 10), query)
								require.Len(t, args, 0)
								return nil, errTest
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: fmt.Sprintf("querying table rows: %v", errTest),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - getting row values",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								return &pgmocks.Rows{
									CloseFn:             func() {},
									NextFn:              func(i uint) bool { return i == 1 },
									ValuesFn:            func() ([]any, error) { return nil, errTest },
									FieldDescriptionsFn: func() []pgconn.FieldDescription { return []pgconn.FieldDescription{} },
									ErrFn:               func() error { return nil },
								}, nil
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: fmt.Sprintf("retrieving rows values: %v", errTest),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - rows err",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT pg_export_snapshot()", query)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 2:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
								require.Equal(t, "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2", query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										pageCount, ok := args[0].(*int)
										require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
										*pageCount = testPageCount
										return nil
									},
								}
							},
						}
						return f(&mockTx)
					case 3:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								return &pgmocks.Rows{
									CloseFn:             func() {},
									NextFn:              func(i uint) bool { return i == 1 },
									ValuesFn:            func() ([]any, error) { return []any{}, nil },
									FieldDescriptionsFn: func() []pgconn.FieldDescription { return []pgconn.FieldDescription{} },
									ErrFn:               func() error { return errTest },
								}, nil
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: errTest.Error(),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
		{
			name: "error - multiple tables and multiple workers",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					mockTx := pgmocks.Tx{
						QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
							if query == "SELECT pg_export_snapshot()" {
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										require.Len(t, args, 1)
										snapshotID, ok := args[0].(*string)
										require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", args[0]))
										*snapshotID = testSnapshotID
										return nil
									},
								}
							}
							if query == "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2" {
								return &pgmocks.Row{
									ScanFn: func(args ...any) error {
										return errTest
									},
								}
							}
							return &pgmocks.Row{
								ScanFn: func(args ...any) error { return fmt.Errorf("unexpected call to QueryRowFn: %s", query) },
							}
						},
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							require.Len(t, args, 0)
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
			},
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable1, testTable2},
				},
			},
			schemaWorkers: 2,

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						testTable1: fmt.Sprintf("getting page count for table %s.%s: %v", testSchema, testTable1, errTest),
						testTable2: fmt.Sprintf("getting page count for table %s.%s: %v", testSchema, testTable2, errTest),
					},
				},
			},
			wantRows: []*snapshot.Row{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rowChan := make(chan *snapshot.Row, 10)
			sg := SnapshotGenerator{
				logger: zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
					LogLevel: "debug",
				})),
				conn:   tc.querier,
				mapper: pglib.NewMapper(tc.querier),
				rowsProcessor: &mocks.RowsProcessor{
					ProcessRowFn: func(ctx context.Context, e *snapshot.Row) error {
						rowChan <- e
						return nil
					},
				},
				schemaWorkers:   1,
				tableWorkers:    1,
				batchPageSize:   10,
				snapshotWorkers: 1,
			}
			sg.tableSnapshotGenerator = sg.snapshotTable

			if tc.schemaWorkers != 0 {
				sg.schemaWorkers = tc.schemaWorkers
			}

			s := testSnapshot
			if tc.snapshot != nil {
				s = tc.snapshot
			}

			err := sg.CreateSnapshot(context.Background(), s)
			require.Equal(t, tc.wantErr, err)
			close(rowChan)

			rows := []*snapshot.Row{}
			for row := range rowChan {
				rows = append(rows, row)
			}
			diff := cmp.Diff(rows, tc.wantRows,
				cmpopts.IgnoreFields(wal.Data{}, "Timestamp"),
				cmpopts.SortSlices(func(a, b *snapshot.Row) bool { return a.Table < b.Table }))
			require.Empty(t, diff, fmt.Sprintf("got: \n%v, \nwant \n%v, \ndiff: \n%s", rows, tc.wantRows, diff))
		})
	}
}
