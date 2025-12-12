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
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
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

func TestNewBatchWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  *Config
		wantErr error
	}{
		{
			name: "error - schema log store not provided",
			config: &Config{
				IgnoreDDL: false,
				SchemaLogStore: schemalogpg.Config{
					URL: "",
				},
			},
			wantErr: errSchemaLogStoreNotProvided,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := NewBatchWriter(ctx, tc.config)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

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
				s.SendMessageFn = func(ctx context.Context, w *batch.WALMessage[*query]) error { return errTest }
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
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
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
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
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
						ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
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
				Writer: &Writer{
					logger:       loglib.NewNoopLogger(),
					pgConn:       tc.pgconn,
					checkpointer: tc.checkpointer,
					adapter:      &mockAdapter{},
				},
				batchSender: tc.batchSender,
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
