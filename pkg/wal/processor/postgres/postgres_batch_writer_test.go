// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	batchmocks "github.com/xataio/pgstream/pkg/wal/processor/batch/mocks"
)

var (
	testSchema = "test_schema"
	testTable  = "test_table"

	testLSNStr         = "1/CF54A048"
	testCommitPosition = wal.CommitPosition(testLSNStr)

	errTest = errors.New("oh noes")
)

func TestBatchWriter_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testWalEvent := &wal.Event{
		Data: &wal.Data{
			Action: "I",
			LSN:    testLSNStr,
			Schema: testSchema,
			Table:  testTable,
		},
		CommitPosition: testCommitPosition,
	}

	testMessage := &walMessage{
		data: testWalEvent.Data,
		schemaInfo: schemaInfo{
			generatedColumns: map[string]struct{}{},
			sequenceColumns:  map[string]string{},
		},
	}

	tests := []struct {
		name        string
		walEvent    *wal.Event
		batchSender *batchmocks.BatchSender[*walMessage]
		adapter     walAdapter

		wantMsgs []*batch.WALMessage[*walMessage]
		wantErr  error
	}{
		{
			name:        "ok",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[*walMessage](),
			adapter: &mockAdapter{
				walEventToMessageFn: func(e *wal.Event) (*walMessage, error) {
					require.Equal(t, e, testWalEvent)
					return testMessage, nil
				},
			},

			wantMsgs: []*batch.WALMessage[*walMessage]{
				batch.NewWALMessage(testMessage, testCommitPosition),
			},
			wantErr: nil,
		},
		{
			name:        "error - event to message",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[*walMessage](),
			adapter: &mockAdapter{
				walEventToMessageFn: func(e *wal.Event) (*walMessage, error) {
					return nil, errTest
				},
			},

			wantMsgs: []*batch.WALMessage[*walMessage]{},
			wantErr:  errTest,
		},
		{
			name:     "error - adding to batch",
			walEvent: testWalEvent,
			batchSender: func() *batchmocks.BatchSender[*walMessage] {
				s := batchmocks.NewBatchSender[*walMessage]()
				s.SendMessageFn = func(ctx context.Context, w *batch.WALMessage[*walMessage]) error { return errTest }
				return s
			}(),
			adapter: &mockAdapter{
				walEventToMessageFn: func(e *wal.Event) (*walMessage, error) {
					require.Equal(t, e, testWalEvent)
					return testMessage, nil
				},
			},

			wantMsgs: []*batch.WALMessage[*walMessage]{},
			wantErr:  errTest,
		},
		{
			name:        "error - panic recovery",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[*walMessage](),
			adapter: &mockAdapter{
				walEventToMessageFn: func(e *wal.Event) (*walMessage, error) {
					panic(errTest)
				},
			},

			wantMsgs: []*batch.WALMessage[*walMessage]{},
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				Writer: &Writer{
					logger:  loglib.NewNoopLogger(),
					adapter: tc.adapter,
				},
				batchSender: tc.batchSender,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				defer tc.batchSender.Close()
				err := writer.ProcessWALEvent(ctx, tc.walEvent)
				if !errors.Is(err, tc.wantErr) {
					require.Equal(t, err.Error(), tc.wantErr.Error())
				}
			}()

			msgs := tc.batchSender.GetWALMessages()
			require.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestBatchWriter_sendBatch(t *testing.T) {
	t.Parallel()

	newDMLMsg := func(action string, schema, table string, identity []wal.Column) *walMessage {
		return &walMessage{
			data: &wal.Data{
				Action:   action,
				Schema:   schema,
				Table:    table,
				Identity: identity,
			},
		}
	}

	deleteMsg := func(id any) *walMessage {
		return newDMLMsg("D", testSchema, testTable, []wal.Column{
			{Name: "id", Type: "bigint", Value: id},
		})
	}

	tests := []struct {
		name         string
		pgconn       *pgmocks.Querier
		adapter      walAdapter
		dmlAdapter   *dmlAdapter
		batch        *batch.Batch[*walMessage]
		checkpointer checkpointer.Checkpoint

		wantErr error
	}{
		{
			name: "ok - single insert message",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			adapter:    &mockAdapter{},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				{
					data: &wal.Data{
						Action: "I",
						Schema: testSchema,
						Table:  testTable,
						Columns: []wal.Column{
							{Name: "id", Type: "bigint", Value: float64(1)},
							{Name: "name", Type: "text", Value: "alice"},
						},
					},
				},
			}, []wal.CommitPosition{testCommitPosition}),

			wantErr: nil,
		},
		{
			name: "ok - coalesced deletes",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
							require.True(t, strings.Contains(sql, "ANY"), "expected bulk delete with ANY, got: %s", sql)
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			adapter:    &mockAdapter{},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				deleteMsg(float64(1)),
				deleteMsg(float64(2)),
				deleteMsg(float64(3)),
			}, []wal.CommitPosition{testCommitPosition}),

			wantErr: nil,
		},
		{
			name: "ok - DDL flushes pending DML",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
				ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
					require.Equal(t, "ALTER TABLE test_schema.test_table ADD COLUMN x text", sql)
					return pglib.CommandTag{}, nil
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					return []*query{{
						schema: testSchema,
						table:  testTable,
						sql:    "ALTER TABLE test_schema.test_table ADD COLUMN x text",
						isDDL:  true,
					}}, nil
				},
			},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				deleteMsg(float64(1)),
				{data: &wal.Data{Action: "M", Prefix: "pgstream.ddl", Schema: testSchema, Table: testTable}, isDDL: true},
			}, []wal.CommitPosition{testCommitPosition}),

			wantErr: nil,
		},
		{
			name: "error - executing query",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, errTest
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			adapter:    &mockAdapter{},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				deleteMsg(float64(1)),
			}, []wal.CommitPosition{testCommitPosition}),

			wantErr: errTest,
		},
		{
			name: "error - checkpointing",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			adapter:    &mockAdapter{},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				deleteMsg(float64(1)),
			}, []wal.CommitPosition{testCommitPosition}),
			checkpointer: func(ctx context.Context, positions []wal.CommitPosition) error { return errTest },

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				Writer: &Writer{
					logger:       loglib.NewNoopLogger(),
					pgConn:       tc.pgconn,
					checkpointer: tc.checkpointer,
					adapter:      tc.adapter,
				},
				dmlAdapter:  tc.dmlAdapter,
				batchSender: batchmocks.NewBatchSender[*walMessage](),
			}
			defer writer.Close()

			err := writer.sendBatch(context.Background(), tc.batch)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestBatchWriter_flushQueries(t *testing.T) {
	t.Parallel()

	testQuerySQL := "INSERT INTO test(id, name) VALUES($1, $2)"
	args1 := []any{1, "alice"}
	args2 := []any{2, "bob"}

	testQuery := func(args []any) *query {
		return &query{
			sql:  testQuerySQL,
			args: args,
		}
	}
	execCalls := uint(0)

	tests := []struct {
		name            string
		pgconn          *pgmocks.Querier
		queries         []*query
		disableTriggers bool

		wantExecCalls uint
		wantErr       error
	}{
		{
			name: "ok - no queries",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					return errors.New("ExecInTxFn: should not be called")
				},
			},
			queries: []*query{},

			wantExecCalls: 0,
			wantErr:       nil,
		},
		{
			name: "ok",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							switch i {
							case 1:
								require.Equal(t, testQuerySQL, query)
								require.Equal(t, args1, args)
								return pglib.CommandTag{}, nil
							case 2:
								require.Equal(t, testQuerySQL, query)
								require.Equal(t, args2, args)
								return pglib.CommandTag{}, &pglib.ErrConstraintViolation{}
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected call to tx ExecFn: %v", args[1])
							}
						},
					}
					return f(&mockTx)
				},
			},
			queries: []*query{testQuery(args1), testQuery(args2)},

			wantExecCalls: 3,
			wantErr:       nil,
		},
		{
			name: "ok - disable triggers",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							switch i {
							case 1:
								require.Equal(t, "SET session_replication_role = replica", query)
							case 2:
								require.Equal(t, testQuerySQL, query)
								require.Len(t, args, 2)
								if args[0] != args1[0] && args[0] != args2[0] {
									return pglib.CommandTag{}, fmt.Errorf("unexpected arguments in query: %v", args)
								}
								if args[0] == args1[0] {
									require.Equal(t, args1, args)
								}
								// the second time it's called we don't return a retriable error
								if args[0] == args2[0] {
									require.Equal(t, args2, args)
								}
							case 3:
								if query == testQuerySQL {
									require.Equal(t, args2, args)
									return pglib.CommandTag{}, &pglib.ErrConstraintViolation{}
								}
								require.Equal(t, "SET session_replication_role = DEFAULT", query)
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected call to tx ExecFn: %v", args[1])
							}
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
			},
			queries:         []*query{testQuery(args1), testQuery(args2)},
			disableTriggers: true,

			wantExecCalls: 6,
			wantErr:       nil,
		},
		{
			name: "error - internal error in tx exec",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							switch i {
							case 1:
								require.Equal(t, testQuerySQL, query)
								require.Equal(t, args1, args)
								return pglib.CommandTag{}, nil
							case 2:
								require.Equal(t, testQuerySQL, query)
								require.Equal(t, args2, args)
								return pglib.CommandTag{}, errTest
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected call to tx ExecFn: %v", args[1])
							}
						},
					}
					return f(&mockTx)
				},
			},
			queries: []*query{testQuery(args1), testQuery(args2)},

			wantExecCalls: 2,
			wantErr:       errTest,
		},
		{
			name: "error - setting replication role to replica",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							switch i {
							case 1:
								require.Equal(t, "SET session_replication_role = replica", query)
								return pglib.CommandTag{}, errTest
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected call to tx ExecFn: %v", args[1])
							}
						},
					}
					return f(&mockTx)
				},
			},
			queries:         []*query{testQuery(args1), testQuery(args2)},
			disableTriggers: true,

			wantExecCalls: 1,
			wantErr:       errTest,
		},
		{
			name: "error - resetting replication role to replica",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							switch i {
							case 1:
								require.Equal(t, "SET session_replication_role = replica", query)
							case 2:
								require.Equal(t, testQuerySQL, query)
								require.Equal(t, args1, args)
							case 3:
								require.Equal(t, "SET session_replication_role = DEFAULT", query)
								return pglib.CommandTag{}, errTest
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected call to tx ExecFn: %v", args[1])
							}
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
			},
			queries:         []*query{testQuery(args1)},
			disableTriggers: true,

			wantExecCalls: 3,
			wantErr:       errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bw := &BatchWriter{
				Writer: &Writer{
					logger:          loglib.NewNoopLogger(),
					pgConn:          tc.pgconn,
					disableTriggers: tc.disableTriggers,
				},
			}

			err := bw.flushQueries(context.Background(), tc.queries)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantExecCalls, execCalls)
			execCalls = 0
		})
	}
}

func mustNewDMLAdapter(t *testing.T) *dmlAdapter {
	t.Helper()
	a, err := newDMLAdapter("", false, loglib.NewNoopLogger())
	require.NoError(t, err)
	return a
}
