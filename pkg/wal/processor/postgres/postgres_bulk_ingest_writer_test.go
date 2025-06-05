// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	batchmocks "github.com/xataio/pgstream/pkg/wal/processor/batch/mocks"
	"golang.org/x/sync/errgroup"
)

func TestBulkIngestWriter_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testSchema1 := "test_schema_1"
	testTable1 := "test_table_1"
	testSchema2 := "test_schema_2"
	testTable2 := "test_table_2"

	testTable1Key := pglib.QuoteQualifiedIdentifier(testSchema1, testTable1)
	testTable2Key := pglib.QuoteQualifiedIdentifier(testSchema2, testTable2)

	testWalEvent := func(schema, table string) *wal.Event {
		return &wal.Event{
			Data: &wal.Data{
				Action: "I",
				LSN:    testLSNStr,
				Schema: schema,
				Table:  table,
			},
			CommitPosition: testCommitPosition,
		}
	}

	testInsertQuery := func(schema, table string) *query {
		return &query{
			schema:      schema,
			table:       table,
			columnNames: []string{"id", "name"},
			sql:         "INSERT INTO test(id, name) VALUES($1, $2) ON CONFLICT (id) DO NOTHING",
			args:        []any{1, "alice"},
		}
	}

	testAdapter := &mockAdapter{
		walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
			switch e.Data.Schema {
			case testSchema1:
				require.Equal(t, e, testWalEvent(testSchema1, testTable1))
				return []*query{testInsertQuery(testSchema1, testTable1)}, nil
			case testSchema2:
				require.Equal(t, e, testWalEvent(testSchema2, testTable2))
				return []*query{testInsertQuery(testSchema2, testTable2)}, nil
			default:
				return nil, fmt.Errorf("unexpected event received: %v", e)
			}
		},
	}

	tests := []struct {
		name               string
		walEvents          []*wal.Event
		batchSenderMap     map[string]queryBatchSender
		adapter            walAdapter
		batchSenderBuilder func(ctx context.Context) (queryBatchSender, error)

		wantMsgs map[string][]*batch.WALMessage[*query]
		wantErr  error
	}{
		{
			name: "ok - multiple events for different tables",
			walEvents: []*wal.Event{
				testWalEvent(testSchema1, testTable1),
				testWalEvent(testSchema2, testTable2),
			},
			batchSenderMap: map[string]queryBatchSender{
				testTable1Key: batchmocks.NewBatchSender[*query](),
				testTable2Key: batchmocks.NewBatchSender[*query](),
			},
			adapter: testAdapter,
			batchSenderBuilder: func(ctx context.Context) (queryBatchSender, error) {
				return nil, errors.New("batchSenderBuilder should not be called")
			},

			wantMsgs: map[string][]*batch.WALMessage[*query]{
				testTable1Key: {batch.NewWALMessage(testInsertQuery(testSchema1, testTable1), testCommitPosition)},
				testTable2Key: {batch.NewWALMessage(testInsertQuery(testSchema2, testTable2), testCommitPosition)},
			},
			wantErr: nil,
		},
		{
			name: "ok - non insert events skipped",
			walEvents: []*wal.Event{
				{
					Data: &wal.Data{
						Action: "U",
						LSN:    testLSNStr,
						Schema: testSchema1,
						Table:  testTable1,
					},
					CommitPosition: testCommitPosition,
				},
			},
			batchSenderMap: nil,
			adapter:        testAdapter,
			batchSenderBuilder: func(ctx context.Context) (queryBatchSender, error) {
				return nil, errors.New("batchSenderBuilder should not be called")
			},

			wantMsgs: nil,
			wantErr:  nil,
		},
		{
			name:           "error - event to query",
			walEvents:      []*wal.Event{testWalEvent(testSchema1, testTable1)},
			batchSenderMap: nil,
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					return nil, errTest
				},
			},
			batchSenderBuilder: func(ctx context.Context) (queryBatchSender, error) {
				return nil, errors.New("batchSenderBuilder should not be called")
			},

			wantMsgs: nil,
			wantErr:  errTest,
		},
		{
			name:           "error - creating batch sender",
			walEvents:      []*wal.Event{testWalEvent(testSchema1, testTable1)},
			batchSenderMap: nil,
			adapter:        testAdapter,
			batchSenderBuilder: func(ctx context.Context) (queryBatchSender, error) {
				return nil, errTest
			},

			wantMsgs: nil,
			wantErr:  errTest,
		},
		{
			name:           "error - sending message",
			walEvents:      []*wal.Event{testWalEvent(testSchema1, testTable1)},
			batchSenderMap: make(map[string]queryBatchSender),
			adapter:        testAdapter,
			batchSenderBuilder: func(ctx context.Context) (queryBatchSender, error) {
				s := batchmocks.NewBatchSender[*query]()
				s.SendMessageFn = func(ctx context.Context, w *batch.WALMessage[*query]) error {
					return errTest
				}
				return s, nil
			},

			wantMsgs: nil,
			wantErr:  errTest,
		},
		{
			name:           "error - panic recovery",
			walEvents:      []*wal.Event{testWalEvent(testSchema1, testTable1)},
			batchSenderMap: nil,
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					panic(errTest)
				},
			},

			wantMsgs: nil,
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BulkIngestWriter{
				Writer: &Writer{
					logger:  loglib.NewNoopLogger(),
					adapter: tc.adapter,
				},
				batchSenderMap:      tc.batchSenderMap,
				batchSenderMapMutex: &sync.RWMutex{},
				batchSenderBuilder:  tc.batchSenderBuilder,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				defer func() {
					for _, sender := range writer.batchSenderMap {
						sender.Close()
					}
				}()
				for _, evt := range tc.walEvents {
					err := writer.ProcessWALEvent(ctx, evt)
					require.ErrorIs(t, err, tc.wantErr)
				}
			}()

			eg := errgroup.Group{}
			for key, sender := range writer.batchSenderMap {
				eg.Go(func() error {
					s, ok := sender.(*batchmocks.BatchSender[*query])
					require.True(t, ok)
					msgs := s.GetWALMessages()
					require.Equal(t, tc.wantMsgs[key], msgs)
					return nil
				})
			}
			require.NoError(t, eg.Wait())
		})
	}
}

func TestBulkIngestWriter_sendBatch(t *testing.T) {
	t.Parallel()

	testSchema := "test_table"
	testTable := "test_schema"

	testQuery := &query{
		schema:      testSchema,
		table:       testTable,
		columnNames: []string{`"id"`, `"name"`},
		sql:         "INSERT INTO users(id, name) VALUES($1, $2)",
		args:        []any{1, "alice"},
	}

	tests := []struct {
		name            string
		batch           *batch.Batch[*query]
		pgConn          *pgmocks.Querier
		disableTriggers bool

		wantErr error
	}{
		{
			name:  "ok - empty batch",
			batch: batch.NewBatch([]*query{}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							return -1, errors.New("unexpected call to CopyFrom with empty batch")
						},
					}
					return f(tx)
				},
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					return nil, errors.New("unexpected call to QueryFn with empty batch")
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							require.Equal(t, pglib.QuoteQualifiedIdentifier(testSchema, testTable), tableName)
							require.Equal(t, testQuery.columnNames, columnNames)
							require.Equal(t, testQuery.args, rows[0])
							return 1, nil
						},
					}
					return f(tx)
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - disable triggers",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
							switch i {
							case 1:
								require.Equal(t, "SET session_replication_role = replica", s)
							case 2:
								require.Equal(t, "SET session_replication_role = DEFAULT", s)
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected number of calls to tx ExecFn: %d", i)
							}
							return pglib.CommandTag{}, nil
						},
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							require.Equal(t, pglib.QuoteQualifiedIdentifier(testSchema, testTable), tableName)
							require.Equal(t, testQuery.columnNames, columnNames)
							require.Equal(t, testQuery.args, rows[0])
							return 1, nil
						},
					}
					return f(tx)
				},
			},
			disableTriggers: true,

			wantErr: nil,
		},
		{
			name:  "error - copying from",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							return 0, errTest
						},
					}
					return f(tx)
				},
			},

			wantErr: errTest,
		},
		{
			name:  "error - rows copied mismatch",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							return 0, nil
						},
					}
					return f(tx)
				},
			},

			wantErr: errUnexpectedCopiedRows,
		},
		{
			name:  "error - setting replication role to replica",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
							switch i {
							case 1:
								require.Equal(t, "SET session_replication_role = replica", s)
								return pglib.CommandTag{}, errTest
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected number of calls to tx ExecFn: %d", i)
							}
						},
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							return 0, errors.New("CopyFrom should not be called")
						},
					}
					return f(tx)
				},
			},
			disableTriggers: true,

			wantErr: errTest,
		},
		{
			name:  "error - resetting replication role",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					tx := &pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
							switch i {
							case 1:
								require.Equal(t, "SET session_replication_role = replica", s)
								return pglib.CommandTag{}, nil
							case 2:
								require.Equal(t, "SET session_replication_role = DEFAULT", s)
								return pglib.CommandTag{}, errTest
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected number of calls to tx ExecFn: %d", i)
							}
						},
						CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
							return 1, nil
						},
					}
					return f(tx)
				},
			},
			disableTriggers: true,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BulkIngestWriter{
				Writer: &Writer{
					logger:          loglib.NewNoopLogger(),
					pgConn:          tc.pgConn,
					disableTriggers: tc.disableTriggers,
				},
			}

			err := writer.sendBatch(context.Background(), tc.batch)
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
		})
	}
}

// func TestBulkIngestWriter_getSrcRows(t *testing.T) {
// 	t.Parallel()

// 	tests := []struct {
// 		name        string
// 		inserts     []*query
// 		columnNames []string

// 		wantRows [][]any
// 	}{
// 		{
// 			name: "ok - no generated columns",
// 			inserts: []*query{
// 				{
// 					columnNames: []string{`"a"`, `"b"`, `"c"`},
// 					args:        []any{"1", "2", "3"},
// 				},
// 			},
// 			columnNames: []string{"a", "b", "c"},

// 			wantRows: [][]any{
// 				{"1", "2", "3"},
// 			},
// 		},
// 		{
// 			name: "ok - with generated columns",
// 			inserts: []*query{
// 				{
// 					columnNames: []string{`"a"`, `"b"`, `"c"`, `"d"`, `"e"`},
// 					args:        []any{"1", "2", "3", "4", "5"},
// 				},
// 			},
// 			columnNames: []string{"a", "c", "d"},

// 			wantRows: [][]any{
// 				{"1", "3", "4"},
// 			},
// 		},
// 	}

// 	for _, tc := range tests {
// 		t.Run(tc.name, func(t *testing.T) {
// 			t.Parallel()

// 			w := &BulkIngestWriter{}

// 			rows := w.getSrcRows(tc.inserts, tc.columnNames)
// 			require.Equal(t, tc.wantRows, rows)
// 		})
// 	}
// }
