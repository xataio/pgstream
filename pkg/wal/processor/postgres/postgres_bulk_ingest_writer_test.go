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
		columnNames: []string{"id", "name"},
		sql:         "INSERT INTO users(id, name) VALUES($1, $2)",
		args:        []any{1, "alice"},
	}

	tests := []struct {
		name   string
		batch  *batch.Batch[*query]
		pgConn *pgmocks.Querier

		wantErr error
	}{
		{
			name:  "ok - empty batch",
			batch: batch.NewBatch([]*query{}, nil),
			pgConn: &pgmocks.Querier{
				CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
					return -1, errors.New("unexpected call to CopyFrom with empty batch")
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
					require.Equal(t, pglib.QuoteQualifiedIdentifier(testSchema, testTable), tableName)
					require.Equal(t, testQuery.columnNames, columnNames)
					require.Equal(t, testQuery.args, rows[0])
					return 1, nil
				},
			},

			wantErr: nil,
		},
		{
			name:  "error - copying from",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
					return 0, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name:  "error - rows copied mismatch",
			batch: batch.NewBatch([]*query{testQuery}, nil),
			pgConn: &pgmocks.Querier{
				CopyFromFn: func(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error) {
					return 0, nil
				},
			},

			wantErr: errors.New("number of rows copied (0) doesn't match the source rows (1)"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := &BulkIngestWriter{
				Writer: &Writer{
					logger: loglib.NewNoopLogger(),
					pgConn: tc.pgConn,
				},
			}

			err := writer.sendBatch(context.Background(), tc.batch)
			if errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
		})
	}
}
