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

	rowColumns := func(id any, name string) []wal.Column {
		return []wal.Column{
			{Name: "id", Type: "bigint", Value: id},
			{Name: "name", Type: "text", Value: name},
		}
	}

	insertMsg := func(id any, name string) *walMessage {
		return &walMessage{data: &wal.Data{
			Action:  "I",
			Schema:  testSchema,
			Table:   testTable,
			Columns: rowColumns(id, name),
		}}
	}

	updateMsg := func(id any, name string) *walMessage {
		return &walMessage{data: &wal.Data{
			Action:   "U",
			Schema:   testSchema,
			Table:    testTable,
			Columns:  rowColumns(id, name),
			Identity: []wal.Column{{Name: "id", Type: "bigint", Value: id}},
		}}
	}

	// recordingConn captures the statements a case executes in order, so a case
	// can assert on how the batch was split rather than only on the error.
	recordingConn := func(executed *[]string) *pgmocks.Querier {
		return &pgmocks.Querier{
			ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
				mockTx := pgmocks.Tx{
					ExecFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.CommandTag, error) {
						*executed = append(*executed, sql)
						return pglib.CommandTag{}, nil
					},
				}
				return f(&mockTx)
			},
			CloseFn: func(ctx context.Context) error { return nil },
		}
	}

	var runBoundarySQL, updateRunSQL []string

	tests := []struct {
		name         string
		pgconn       *pgmocks.Querier
		adapter      walAdapter
		dmlAdapter   *dmlAdapter
		batch        *batch.Batch[*walMessage]
		checkpointer checkpointer.Checkpoint

		assert  func(t *testing.T)
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
			// a run is the unit of coalescing, so the boundary between two of
			// them is what keeps a delete from being folded into the inserts
			// around it. Without the flush on action change the two inserts
			// would merge across the delete and the rows would land in the
			// wrong order.
			name:       "ok - a change of action flushes the pending run",
			pgconn:     recordingConn(&runBoundarySQL),
			adapter:    &mockAdapter{},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				insertMsg(float64(1), "a"),
				insertMsg(float64(2), "b"),
				deleteMsg(float64(3)),
				insertMsg(float64(4), "d"),
			}, []wal.CommitPosition{testCommitPosition}),

			assert: func(t *testing.T) {
				require.Len(t, runBoundarySQL, 3, "expected three runs: insert, delete, insert")

				require.Contains(t, runBoundarySQL[0], "INSERT INTO")
				// two rows of two columns coalesced into one statement
				require.Contains(t, runBoundarySQL[0], "$4")
				require.NotContains(t, runBoundarySQL[0], "$5")

				require.Contains(t, runBoundarySQL[1], "DELETE FROM")
				require.Contains(t, runBoundarySQL[1], "ANY")

				// the trailing insert is its own run, not appended to the first
				require.Contains(t, runBoundarySQL[2], "INSERT INTO")
				require.Contains(t, runBoundarySQL[2], "$2")
				require.NotContains(t, runBoundarySQL[2], "$3")
			},
			wantErr: nil,
		},
		{
			// updates take the default branch of buildCoalescedQueries: they
			// are emitted one statement per event rather than coalesced
			name:       "ok - updates are not coalesced",
			pgconn:     recordingConn(&updateRunSQL),
			adapter:    &mockAdapter{},
			dmlAdapter: mustNewDMLAdapter(t),
			batch: batch.NewBatch([]*walMessage{
				updateMsg(float64(1), "a"),
				updateMsg(float64(2), "b"),
			}, []wal.CommitPosition{testCommitPosition}),

			assert: func(t *testing.T) {
				require.Len(t, updateRunSQL, 2, "each update is its own statement")
				for _, sql := range updateRunSQL {
					require.Contains(t, sql, "UPDATE")
					require.Contains(t, sql, "WHERE")
				}
			},
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

			if tc.assert != nil {
				tc.assert(t)
			}
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
			// #1037: a client-side encoding failure is deterministic and
			// attributable to one query, so it must drop that query and retry
			// the rest rather than failing the whole send — which, with
			// ignore_send_errors, dropped co-batched writes to other tables.
			name: "ok - value encoding error drops only the failing query",
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
								return pglib.CommandTag{}, fmt.Errorf("executing query: %w",
									&pglib.ErrValueEncoding{Details: `failed to encode args[1]: unable to encode []interface {}{"BiteScan"}`})
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected call to tx ExecFn: %v", args[1])
							}
						},
					}
					return f(&mockTx)
				},
			},
			queries: []*query{testQuery(args1), testQuery(args2)},

			// query1, failing query2, then query1 again on the retry
			wantExecCalls: 3,
			wantErr:       nil,
		},
		{
			// the retrier re-invokes the tx closure, so state recorded by a
			// failed attempt must not leak into a later successful one:
			// attempt 1 fails on query 2, attempt 2 commits both, and nothing
			// may be reported as needing a retry or as dropped.
			name: "ok - retried tx that succeeds reports nothing to retry",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					attempt := 0
					var runAttempt func() error
					runAttempt = func() error {
						attempt++
						failing := attempt == 1
						mockTx := pgmocks.Tx{
							ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
								execCalls++
								if failing && i == 2 {
									// transient failure, not attributable to the query
									return pglib.CommandTag{}, errTest
								}
								return pglib.CommandTag{}, nil
							},
						}
						if err := f(&mockTx); err != nil && attempt == 1 {
							return runAttempt()
						}
						return nil
					}
					return runAttempt()
				},
			},
			queries: []*query{testQuery(args1), testQuery(args2)},

			// attempt 1: query1, query2 (fails); attempt 2: query1, query2
			wantExecCalls: 4,
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

func TestBatchWriter_execQueries_strictMode(t *testing.T) {
	t.Parallel()

	testQuerySQL := "INSERT INTO test(id, name) VALUES($1, $2)"
	testQuery := func(args []any) *query {
		return &query{sql: testQuerySQL, args: args}
	}

	// pgConn where the first query succeeds and the second fails with a
	// non-internal (DATALOSS) constraint violation.
	newPgConn := func() *pgmocks.Querier {
		return &pgmocks.Querier{
			ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
				mockTx := pgmocks.Tx{
					ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
						if i == 1 {
							return pglib.CommandTag{}, nil
						}
						return pglib.CommandTag{}, &pglib.ErrConstraintViolation{}
					},
				}
				return f(&mockTx)
			},
		}
	}

	queries := []*query{testQuery([]any{1, "alice"}), testQuery([]any{2, "bob"})}

	t.Run("default mode drops and continues", func(t *testing.T) {
		t.Parallel()
		bw := &BatchWriter{
			Writer: &Writer{
				logger: loglib.NewNoopLogger(),
				pgConn: newPgConn(),
			},
		}

		retry, err := bw.execQueries(context.Background(), queries)
		require.NoError(t, err)
		// the failing query is dropped, the succeeding one is returned for retry
		require.Len(t, retry, 1)
		require.Equal(t, []any{1, "alice"}, retry[0].args)
		require.Equal(t, uint64(1), bw.DroppedQueries())
	})

	t.Run("strict mode returns the error", func(t *testing.T) {
		t.Parallel()
		bw := &BatchWriter{
			Writer: &Writer{
				logger:     loglib.NewNoopLogger(),
				pgConn:     newPgConn(),
				strictMode: true,
			},
		}

		retry, err := bw.execQueries(context.Background(), queries)
		require.Error(t, err)
		var cv *pglib.ErrConstraintViolation
		require.ErrorAs(t, err, &cv)
		// the message is what a snapshot caller sees when rows would otherwise
		// be dropped silently, so it is part of the contract
		require.ErrorContains(t, err, "strict mode: stopping on non-internal query failure")
		require.Nil(t, retry)
		require.Equal(t, uint64(0), bw.DroppedQueries())
	})
}

func mustNewDMLAdapter(t *testing.T) *dmlAdapter {
	t.Helper()
	a, err := newDMLAdapter("", false, loglib.NewNoopLogger())
	require.NoError(t, err)
	return a
}
