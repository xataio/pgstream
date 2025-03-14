// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
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

	testInsertQuery := &query{
		sql:  "INSERT INTO test(id, name) VALUES($1, $2) ON CONFLICT (id) DO NOTHING",
		args: []any{1, "alice"},
	}

	tests := []struct {
		name        string
		walEvent    *wal.Event
		batchSender *batchmocks.BatchSender[*query]
		adapter     walAdapter

		wantMsgs []*batch.WALMessage[*query]
		wantErr  error
	}{
		{
			name:        "ok",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[*query](),
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					require.Equal(t, e, testWalEvent)
					return []*query{testInsertQuery}, nil
				},
			},

			wantMsgs: []*batch.WALMessage[*query]{
				batch.NewWALMessage(testInsertQuery, testCommitPosition),
			},
			wantErr: nil,
		},
		{
			name:        "error - event to query",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[*query](),
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					return nil, errTest
				},
			},

			wantMsgs: []*batch.WALMessage[*query]{},
			wantErr:  errTest,
		},
		{
			name:     "error - adding to batch",
			walEvent: testWalEvent,
			batchSender: func() *batchmocks.BatchSender[*query] {
				s := batchmocks.NewBatchSender[*query]()
				s.AddToBatchFn = func(ctx context.Context, w *batch.WALMessage[*query]) error { return errTest }
				return s
			}(),
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					require.Equal(t, e, testWalEvent)
					return []*query{testInsertQuery}, nil
				},
			},

			wantMsgs: []*batch.WALMessage[*query]{},
			wantErr:  errTest,
		},
		{
			name:        "error - panic recovery",
			walEvent:    testWalEvent,
			batchSender: batchmocks.NewBatchSender[*query](),
			adapter: &mockAdapter{
				walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
					panic(errTest)
				},
			},

			wantMsgs: []*batch.WALMessage[*query]{},
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				logger:      loglib.NewNoopLogger(),
				batchSender: tc.batchSender,
				adapter:     tc.adapter,
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

	testQuery := &query{
		sql:  "INSERT INTO test(id, name) VALUES($1, $2)",
		args: []any{1, "alice"},
	}

	tests := []struct {
		name         string
		pgconn       *pgmocks.Querier
		batchSender  *batchmocks.BatchSender[*query]
		batch        *batch.Batch[*query]
		checkpointer checkpointer.Checkpoint

		wantErr error
	}{
		{
			name: "ok",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, query string, args ...any) (pglib.CommandTag, error) {
							require.Equal(t, testQuery.sql, query)
							require.Equal(t, testQuery.args, args)
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			batchSender: batchmocks.NewBatchSender[*query](),
			batch:       batch.NewBatch([]*query{testQuery}, []wal.CommitPosition{testCommitPosition}),

			wantErr: nil,
		},
		{
			name: "error - executing query",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, query string, args ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, errTest
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			batchSender: batchmocks.NewBatchSender[*query](),
			batch:       batch.NewBatch([]*query{testQuery}, []wal.CommitPosition{testCommitPosition}),

			wantErr: errTest,
		},
		{
			name: "error - checkpointing",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, query string, args ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, nil
						},
					}
					return f(&mockTx)
				},
				CloseFn: func(ctx context.Context) error { return nil },
			},
			batchSender:  batchmocks.NewBatchSender[*query](),
			batch:        batch.NewBatch([]*query{testQuery}, []wal.CommitPosition{testCommitPosition}),
			checkpointer: func(ctx context.Context, positions []wal.CommitPosition) error { return errTest },

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BatchWriter{
				logger:       loglib.NewNoopLogger(),
				batchSender:  tc.batchSender,
				pgConn:       tc.pgconn,
				checkpointer: tc.checkpointer,
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
	args2 := []any{1, "bob"}

	testQuery := func(args []any) *query {
		return &query{
			sql:  testQuerySQL,
			args: args,
		}
	}
	execCalls := uint(0)

	tests := []struct {
		name    string
		pgconn  *pgmocks.Querier
		queries []*query

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
						ExecFn: func(ctx context.Context, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							require.Equal(t, testQuerySQL, query)
							require.Len(t, args, 2)
							switch args[1] {
							case args1[1]:
								return pglib.CommandTag{}, nil
							case args2[1]:
								return pglib.CommandTag{}, &pglib.ErrConstraintViolation{}
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected argument in sql query: %v", args[1])
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
			name: "error - internal error in tx exec",
			pgconn: &pgmocks.Querier{
				ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
					mockTx := pgmocks.Tx{
						ExecFn: func(ctx context.Context, query string, args ...any) (pglib.CommandTag, error) {
							execCalls++
							require.Equal(t, testQuerySQL, query)
							require.Len(t, args, 2)
							switch args[1] {
							case args1[1]:
								return pglib.CommandTag{}, nil
							case args2[1]:
								return pglib.CommandTag{}, errTest
							default:
								return pglib.CommandTag{}, fmt.Errorf("unexpected argument in sql query: %v", args[1])
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bw := &BatchWriter{
				logger: loglib.NewNoopLogger(),
				pgConn: tc.pgconn,
			}

			err := bw.flushQueries(context.Background(), tc.queries)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantExecCalls, execCalls)
			execCalls = 0
		})
	}
}

func TestBatchWriter(t *testing.T) {
	t.Parallel()

	conn := &pgmocks.Querier{
		ExecInTxFn: func(ctx context.Context, f func(tx pglib.Tx) error) error {
			mockTx := pgmocks.Tx{
				ExecFn: func(ctx context.Context, query string, args ...any) (pglib.CommandTag, error) {
					time.Sleep(time.Second)
					return pglib.CommandTag{}, errTest
				},
			}
			return f(&mockTx)
		},
		CloseFn: func(ctx context.Context) error { return nil },
	}

	adapter := &mockAdapter{
		walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
			return []*query{{
				sql:  "INSERT INTO test(id, name) VALUES($1, $2) ON CONFLICT (id) DO NOTHING",
				args: []any{1, "alice"},
			}}, nil
		},
	}

	bw := BatchWriter{
		logger:  loglib.NewNoopLogger(),
		pgConn:  conn,
		adapter: adapter,
	}
	defer bw.Close()

	var err error
	bw.batchSender, err = batch.NewSender(&batch.Config{
		BatchTimeout:  time.Second,
		MaxBatchSize:  10,
		MaxBatchBytes: 10000,
	}, bw.sendBatch, bw.logger)
	require.NoError(t, err)

	doneChan := make(chan struct{}, 1)
	go func() {
		err := bw.batchSender.Send(context.Background())
		require.ErrorIs(t, err, errTest)
		doneChan <- struct{}{}
		close(doneChan)
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var processErr error
	for {
		select {
		case <-doneChan:
			require.ErrorIs(t, processErr, errTest)
			return
		case <-timer.C:
			t.Error("test timeout")
			return
		default:
			processErr = bw.ProcessWALEvent(context.Background(), &wal.Event{
				CommitPosition: wal.CommitPosition("1"),
				Data: &wal.Data{
					Action: "I",
				},
			})
		}
	}
}
