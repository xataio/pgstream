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
	"github.com/xataio/pgstream/internal/progress"
	progressmocks "github.com/xataio/pgstream/internal/progress/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	processormocks "github.com/xataio/pgstream/pkg/wal/processor/mocks"
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
	quotedSchemaTable1 := pglib.QuoteQualifiedIdentifier(testSchema, testTable1)
	quotedSchemaTable2 := pglib.QuoteQualifiedIdentifier(testSchema, testTable2)

	txOptions := pglib.TxOptions{
		IsolationLevel: pglib.RepeatableRead,
		AccessMode:     pglib.ReadOnly,
	}

	testSnapshotID := "test-snapshot-id"
	testPageCount := 0 // 0 means 1 page
	testPageAvgBytes := int64(1024)
	testTotalBytes := int64(2048)
	testRowBytes := int64(512)
	testUUID := uuid.New().String()
	testUUID2 := uuid.New().String()
	testColumns := []wal.Column{
		{Name: "id", Type: "uuid", Value: testUUID},
		{Name: "name", Type: "text", Value: "alice"},
	}

	testEvent := func(tableName string, columns []wal.Column) *wal.Event {
		return &wal.Event{
			CommitPosition: wal.CommitPosition(wal.ZeroLSN),
			Data: &wal.Data{
				Action:  "I",
				LSN:     wal.ZeroLSN,
				Schema:  testSchema,
				Table:   tableName,
				Columns: columns,
			},
		}
	}

	validTableInfoScanFn := func(args ...any) error {
		require.Len(t, args, 3)
		pageCount, ok := args[0].(*int)
		require.True(t, ok, fmt.Sprintf("pageCount, expected *int, got %T", args[0]))
		*pageCount = testPageCount
		pageAvgBytes, ok := args[1].(*int64)
		require.True(t, ok, fmt.Sprintf("pageAvgBytes, expected *int64, got %T", args[1]))
		*pageAvgBytes = testPageAvgBytes
		rowAvgBytes, ok := args[2].(*int64)
		require.True(t, ok, fmt.Sprintf("rowAvgBytes, expected *int64, got %T", args[2]))
		*rowAvgBytes = testRowBytes
		return nil
	}

	validMissedRowsScanFn := func(args ...any) error {
		require.Len(t, args, 1)
		rowCount, ok := args[0].(*int)
		require.True(t, ok, fmt.Sprintf("rowCount, expected *int, got %T", args[0]))
		*rowCount = 0
		return nil
	}

	validTableInfoQueryRowFn := func(_ context.Context, dest []any, query string, args ...any) error {
		switch query {
		case tableInfoQuery:
			require.Equal(t, []any{testTable1, testSchema}, args)
			return validTableInfoScanFn(dest...)
		case fmt.Sprintf(pageRangeQueryCount, quotedSchemaTable1, 1, 2):
			return validMissedRowsScanFn(dest...)
		default:
			return fmt.Errorf("unexpected call to QueryRowFn: %s", query)
		}
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name          string
		querier       pglib.Querier
		snapshot      *snapshot.Snapshot
		schemaWorkers uint
		progressBar   *progressmocks.Bar

		wantEvents []*wal.Event
		wantErr    error
	}{
		{
			name: "ok",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 0, 1), query)
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

			wantErr:    nil,
			wantEvents: []*wal.Event{testEvent(testTable1, testColumns)},
		},
		{
			name: "ok - with missed pages",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								switch query {
								case tableInfoQuery:
									require.Equal(t, []any{testTable1, testSchema}, args)
									return validTableInfoScanFn(dest...)
								case fmt.Sprintf(pageRangeQueryCount, quotedSchemaTable1, 1, 2):
									require.Len(t, dest, 1)
									rowCount, ok := dest[0].(*int)
									require.True(t, ok, fmt.Sprintf("rowCount, expected *int, got %T", dest[0]))
									*rowCount = 1
									return nil
								case fmt.Sprintf(pageRangeQueryCount, quotedSchemaTable1, 2, 3):
									require.Len(t, dest, 1)
									rowCount, ok := dest[0].(*int)
									require.True(t, ok, fmt.Sprintf("rowCount, expected *int, got %T", dest[0]))
									*rowCount = 0
									return nil
								default:
									return fmt.Errorf("unexpected call to QueryRowFn: %s", query)
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
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 0, 1), query)
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
					case 4:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 1, 2), query)
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
										return []any{testUUID2, "bob"}, nil
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

			wantErr: nil,
			wantEvents: []*wal.Event{
				testEvent(testTable1, testColumns),
				testEvent(testTable1, []wal.Column{
					{Name: "id", Type: "uuid", Value: testUUID2},
					{Name: "name", Type: "text", Value: "bob"},
				}),
			},
		},
		{
			name: "ok - with progress tracking",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, fmt.Sprintf(tablesBytesQuery, testSchema, "$1"), query)
								require.Equal(t, []any{testTable1}, args)
								require.Len(t, dest, 1)
								totalBytes, ok := dest[0].(*int64)
								require.True(t, ok, fmt.Sprintf("totalBytes, expected *int64, got %T", dest[0]))
								*totalBytes = testTotalBytes
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
						}
						return f(&mockTx)
					case 4:
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
								require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
								require.Len(t, args, 0)
								return pglib.CommandTag{}, nil
							},
							QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 0, 1), query)
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
			progressBar: &progressmocks.Bar{
				Add64Fn: func(n int64) error {
					require.Equal(t, testRowBytes, n) // only 1 row processed
					return nil
				},
			},

			wantErr:    nil,
			wantEvents: []*wal.Event{testEvent(testTable1, testColumns)},
		},
		{
			name: "ok - multiple tables and multiple workers",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					mockTx := pgmocks.Tx{
						QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
							if query == exportSnapshotQuery {
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
							}
							if query == tableInfoQuery {
								return validTableInfoScanFn(dest...)
							}
							if query == fmt.Sprintf(pageRangeQueryCount, quotedSchemaTable1, 1, 2) ||
								query == fmt.Sprintf(pageRangeQueryCount, quotedSchemaTable2, 1, 2) {
								return validMissedRowsScanFn(dest...)
							}
							return fmt.Errorf("unexpected call to QueryRowFn: %s", query)
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

			wantErr:    nil,
			wantEvents: []*wal.Event{testEvent(testTable1, testColumns), testEvent(testTable2, testColumns)},
		},
		{
			name: "ok - unsupported column type",
			querier: &pgmocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 0, 1), query)
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

			wantErr:    nil,
			wantEvents: []*wal.Event{testEvent(testTable1, testColumns)},
		},
		{
			name: "ok - no data",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 0, 1), query)
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

			wantErr:    nil,
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - exporting snapshot",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								return errTest
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},

			wantErr:    snapshot.NewErrors(testSchema, fmt.Errorf("exporting snapshot: %w", errTest)),
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - setting transaction snapshot before table page count",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - getting table page count",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, tableInfoQuery, query)
								require.Equal(t, []any{testTable1, testSchema}, args)
								return errTest
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
						testTable1: fmt.Sprintf("getting page information for table test-schema.test-table-1: %v", errTest),
					},
				},
			},
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - setting transaction snapshot for table range",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - querying range data",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
								require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable1, 0, 1), query)
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
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - getting row values",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - rows err",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(ctx context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: validTableInfoQueryRowFn,
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
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - multiple tables and multiple workers",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					mockTx := pgmocks.Tx{
						QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
							if query == exportSnapshotQuery {
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
							}
							if query == tableInfoQuery {
								return errTest
							}
							return fmt.Errorf("unexpected call to QueryRowFn: %s", query)
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
						testTable1: fmt.Sprintf("getting page information for table %s.%s: %v", testSchema, testTable1, errTest),
						testTable2: fmt.Sprintf("getting page information for table %s.%s: %v", testSchema, testTable2, errTest),
					},
				},
			},
			wantEvents: []*wal.Event{},
		},
		{
			name: "error - adding progress bar",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					require.Equal(t, txOptions, to)
					switch i {
					case 1:
						mockTx := pgmocks.Tx{
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, exportSnapshotQuery, query)
								require.Len(t, dest, 1)
								snapshotID, ok := dest[0].(*string)
								require.True(t, ok, fmt.Sprintf("snapshotID, expected *string, got %T", dest[0]))
								*snapshotID = testSnapshotID
								return nil
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
							QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
								require.Equal(t, fmt.Sprintf(tablesBytesQuery, testSchema, "$1"), query)
								require.Equal(t, []any{testTable1}, args)
								return errTest
							},
						}
						return f(&mockTx)
					default:
						return fmt.Errorf("unexpected call to ExecInTxWithOptions: %d", i)
					}
				},
			},
			progressBar: &progressmocks.Bar{
				Add64Fn: func(n int64) error {
					return errors.New("Add64Fn should not be called")
				},
			},

			wantErr: snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{fmt.Sprintf("retrieving total bytes for schema: %v", errTest)},
				},
			},
			wantEvents: []*wal.Event{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			eventChan := make(chan *wal.Event, 10)
			sg := SnapshotGenerator{
				logger: zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
					LogLevel: "debug",
				})),
				conn:    tc.querier,
				adapter: newAdapter(pglib.NewMapper(tc.querier)),
				processor: &processormocks.Processor{
					ProcessWALEventFn: func(ctx context.Context, e *wal.Event) error {
						eventChan <- e
						return nil
					},
				},
				schemaWorkers:    1,
				tableWorkers:     1,
				batchBytes:       1024 * 1024, // 1MB
				snapshotWorkers:  1,
				progressTracking: tc.progressBar != nil,
				progressBars:     synclib.NewMap[string, progress.Bar](),
				progressBarBuilder: func(totalBytes int64, description string) progress.Bar {
					return tc.progressBar
				},
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
			close(eventChan)

			events := []*wal.Event{}
			for event := range eventChan {
				events = append(events, event)
			}
			diff := cmp.Diff(events, tc.wantEvents,
				cmpopts.IgnoreFields(wal.Data{}, "Timestamp"),
				cmpopts.SortSlices(func(a, b *wal.Event) bool { return a.Data.Table < b.Data.Table }))
			require.Empty(t, diff, fmt.Sprintf("got: \n%v, \nwant \n%v, \ndiff: \n%s", events, tc.wantEvents, diff))
		})
	}
}

func TestTablePageInfo_calculateBatchPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableInfo *tableInfo
		bytes     uint64

		wantPageSize uint
	}{
		{
			name: "no avg page bytes - return total pages",
			tableInfo: &tableInfo{
				pageCount:    1,
				avgPageBytes: 0,
			},
			wantPageSize: 1,
		},
		{
			name: "single page - return total pages",
			tableInfo: &tableInfo{
				pageCount:    0,
				avgPageBytes: 0,
			},
			wantPageSize: 1,
		},
		{
			name: "multiple pages with average page bytes",
			tableInfo: &tableInfo{
				pageCount:    100,
				avgPageBytes: 10,
			},
			bytes:        1000,
			wantPageSize: 100,
		},
		{
			name: "page size != page count",
			tableInfo: &tableInfo{
				pageCount:    100,
				avgPageBytes: 10,
			},
			bytes:        10,
			wantPageSize: 1,
		},
		{
			name: "bytes > avgPageBytes",
			tableInfo: &tableInfo{
				pageCount:    100,
				avgPageBytes: 11,
			},
			bytes:        10,
			wantPageSize: 1,
		},
		{
			name: "batch page size > total pages",
			tableInfo: &tableInfo{
				pageCount:    1,
				avgPageBytes: 10,
			},
			bytes:        1000,
			wantPageSize: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.tableInfo.calculateBatchPageSize(tc.bytes)
			require.Equal(t, tc.wantPageSize, tc.tableInfo.batchPageSize, "wanted page size %d, got %d", tc.wantPageSize, tc.tableInfo.batchPageSize)
		})
	}
}

func TestSnapshotGenerator_snapshotTableRange(t *testing.T) {
	t.Parallel()

	testTable := "test-table"
	testSchema := "test-schema"
	testSnapshotID := "test-snapshot-id"
	testUUID := uuid.New().String()
	quotedSchemaTable := pglib.QuoteQualifiedIdentifier(testSchema, testTable)

	testColumns := []wal.Column{
		{Name: "id", Type: "uuid", Value: testUUID},
		{Name: "name", Type: "text", Value: "alice"},
	}

	testEvent := &wal.Event{
		CommitPosition: wal.CommitPosition(wal.ZeroLSN),
		Data: &wal.Data{
			Action:  "I",
			LSN:     wal.ZeroLSN,
			Schema:  testSchema,
			Table:   testTable,
			Columns: testColumns,
		},
	}

	testPageRange := pageRange{start: 0, end: 5}
	errTest := errors.New("test error")

	tests := []struct {
		name      string
		querier   pglib.Querier
		table     *table
		pageRange pageRange
		processor processor.Processor

		wantEvents []*wal.Event
		wantErr    error
	}{
		{
			name: "ok - single row",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							require.Equal(t, fmt.Sprintf(pageRangeQuery, quotedSchemaTable, 0, 5), query)
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
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{testEvent},
			wantErr:    nil,
		},
		{
			name: "ok - multiple rows",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return &pgmocks.Rows{
								CloseFn: func() {},
								NextFn:  func(i uint) bool { return i <= 2 },
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
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{testEvent, testEvent},
			wantErr:    nil,
		},
		{
			name: "ok - no rows",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return &pgmocks.Rows{
								CloseFn: func() {},
								NextFn:  func(i uint) bool { return false },
								FieldDescriptionsFn: func() []pgconn.FieldDescription {
									return []pgconn.FieldDescription{}
								},
								ValuesFn: func() ([]any, error) {
									return []any{}, nil
								},
								ErrFn: func() error { return nil },
							}, nil
						},
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{},
			wantErr:    nil,
		},
		{
			name: "ok - with progress tracking",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
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
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{testEvent},
			wantErr:    nil,
		},
		{
			name: "error - setting transaction snapshot",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, errTest
						},
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{},
			wantErr:    fmt.Errorf("setting transaction snapshot: %w", errTest),
		},
		{
			name: "error - querying table rows",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return nil, errTest
						},
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{},
			wantErr:    fmt.Errorf("querying table rows: %w", errTest),
		},
		{
			name: "error - retrieving row values",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return &pgmocks.Rows{
								CloseFn: func() {},
								NextFn:  func(i uint) bool { return i == 1 },
								FieldDescriptionsFn: func() []pgconn.FieldDescription {
									return []pgconn.FieldDescription{}
								},
								ValuesFn: func() ([]any, error) {
									return nil, errTest
								},
								ErrFn: func() error { return nil },
							}, nil
						},
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{},
			wantErr:    fmt.Errorf("retrieving rows values: %w", errTest),
		},
		{
			name: "error - rows error",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return &pgmocks.Rows{
								CloseFn: func() {},
								NextFn:  func(i uint) bool { return false },
								FieldDescriptionsFn: func() []pgconn.FieldDescription {
									return []pgconn.FieldDescription{}
								},
								ValuesFn: func() ([]any, error) {
									return []any{}, nil
								},
								ErrFn: func() error { return errTest },
							}, nil
						},
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return nil
				},
			},
			wantEvents: []*wal.Event{},
			wantErr:    errTest,
		},
		{
			name: "error - processing row fails",
			querier: &pgmocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, i uint, f func(tx pglib.Tx) error, to pglib.TxOptions) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", testSnapshotID), query)
							return pglib.CommandTag{}, nil
						},
						QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
							return &pgmocks.Rows{
								CloseFn: func() {},
								NextFn:  func(i uint) bool { return i == 1 },
								FieldDescriptionsFn: func() []pgconn.FieldDescription {
									return []pgconn.FieldDescription{
										{Name: "id", DataTypeOID: pgtype.UUIDOID},
									}
								},
								ValuesFn: func() ([]any, error) {
									return []any{testUUID}, nil
								},
								ErrFn: func() error { return nil },
							}, nil
						},
					}
					return f(&mockTx)
				},
			},
			table: &table{
				schema:  testSchema,
				name:    testTable,
				rowSize: 512,
			},
			pageRange: testPageRange,
			processor: &processormocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, event *wal.Event) error {
					return errTest
				},
			},
			wantEvents: []*wal.Event{},
			wantErr:    fmt.Errorf("processing snapshot row: %w", errTest),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			eventChan := make(chan *wal.Event, 10)
			progressBar := &progressmocks.Bar{
				Add64Fn: func(n int64) error {
					require.Equal(t, tc.table.rowSize, n)
					return nil
				},
			}

			sg := SnapshotGenerator{
				logger: zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
					LogLevel: "debug",
				})),
				conn:    tc.querier,
				adapter: newAdapter(pglib.NewMapper(tc.querier)),
				processor: &processormocks.Processor{
					ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
						if tc.processor != nil {
							if err := tc.processor.ProcessWALEvent(ctx, walEvent); err != nil {
								return err
							}
						}
						eventChan <- walEvent
						return nil
					},
				},
				progressTracking: tc.name == "ok - with progress tracking",
				progressBars:     synclib.NewMap[string, progress.Bar](),
			}

			if sg.progressTracking {
				sg.progressBars.Set(tc.table.schema, progressBar)
			}

			err := sg.snapshotTableRange(context.Background(), testSnapshotID, tc.table, tc.pageRange)
			require.Equal(t, tc.wantErr, err)
			close(eventChan)

			events := []*wal.Event{}
			for event := range eventChan {
				events = append(events, event)
			}
			diff := cmp.Diff(events, tc.wantEvents,
				cmpopts.IgnoreFields(wal.Data{}, "Timestamp"),
				cmpopts.SortSlices(func(a, b *wal.Event) bool { return a.Data.Table < b.Data.Table }))
			require.Empty(t, diff, fmt.Sprintf("got: \n%v, \nwant \n%v, \ndiff: \n%s", events, tc.wantEvents, diff))
		})
	}
}
