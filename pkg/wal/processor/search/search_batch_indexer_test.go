// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	syncmocks "github.com/xataio/pgstream/internal/sync/mocks"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func TestBatchIndexer_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	id := xid.New()
	testSchemaLogEntry := newTestLogEntry(id, now)
	testCommitPos := newTestCommitPosition()
	testSize := 10

	tests := []struct {
		name              string
		weightedSemaphore *syncmocks.WeightedSemaphore
		adapter           *mockAdapter
		event             *wal.Event

		wantMsgs []*msg
		wantErr  error
	}{
		{
			name: "ok",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool {
					require.Equal(t, int64(testSize), i)
					return true
				},
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return &msg{
						schemaChange: testSchemaLogEntry,
						bytesSize:    testSize,
						pos:          testCommitPos,
					}, nil
				},
			},
			event: newTestSchemaChangeEvent("I", id, now),

			wantMsgs: []*msg{
				{
					schemaChange: testSchemaLogEntry,
					bytesSize:    testSize,
					pos:          testCommitPos,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - nil queue item",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return true },
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return nil, nil
				},
			},
			event: newTestDataEvent("I"),

			wantMsgs: nil,
			wantErr:  nil,
		},
		{
			name: "ok - skipped event",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return true },
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return nil, errNilIDValue
				},
			},
			event: newTestSchemaChangeEvent("I", id, now),

			wantMsgs: nil,
			wantErr:  nil,
		},
		{
			name: "ok - waiting for semaphore",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return false },
				AcquireFn:    func(ctx context.Context, i int64) error { return nil },
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return &msg{
						schemaChange: testSchemaLogEntry,
						pos:          testCommitPos,
					}, nil
				},
			},
			event: newTestSchemaChangeEvent("I", id, now),

			wantMsgs: []*msg{
				{
					schemaChange: testSchemaLogEntry,
					pos:          testCommitPos,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - acquiring semaphore",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return false },
				AcquireFn:    func(ctx context.Context, i int64) error { return errTest },
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return &msg{schemaChange: testSchemaLogEntry}, nil
				},
			},
			event: newTestSchemaChangeEvent("I", id, now),

			wantMsgs: nil,
			wantErr:  errTest,
		},
		{
			name: "error - wal data to queue item",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return true },
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return nil, errTest
				},
			},
			event: newTestSchemaChangeEvent("I", id, now),

			wantMsgs: nil,
			wantErr:  errTest,
		},
		{
			name: "error - panic recovery",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return true },
			},
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					panic(errTest)
				},
			},
			event: newTestDataEvent("I"),

			wantMsgs: nil,
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			indexer := &BatchIndexer{
				queueBytesSema: tc.weightedSemaphore,
				msgChan:        make(chan *msg, 100),
				adapter:        tc.adapter,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				defer close(indexer.msgChan)
				err := indexer.ProcessWALEvent(ctx, tc.event)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			msgs := []*msg{}
			for msg := range indexer.msgChan {
				if ctx.Err() != nil {
					t.Log("test timeout reached")
					return
				}
				msgs = append(msgs, msg)
			}
			if len(msgs) == 0 {
				msgs = nil
			}

			if diff := cmp.Diff(msgs, tc.wantMsgs, cmp.AllowUnexported(msg{}, msg{})); diff != "" {
				t.Errorf("got: \n%v, \nwant \n%v, \ndiff: \n%s", msgs, tc.wantMsgs, diff)
			}
		})
	}
}

func TestBatchIndexer_Send(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	id := xid.New()

	testSchemaLogEntry := newTestLogEntry(id, now)
	testDocument := newTestDocument()

	testSize := 10

	tests := []struct {
		name      string
		store     func(doneChan chan struct{}) *mockStore
		msgs      []*msg
		semaphore *syncmocks.WeightedSemaphore

		wantErr error
	}{
		{
			name: "ok - write",
			store: func(doneChan chan struct{}) *mockStore {
				once := sync.Once{}
				return &mockStore{
					sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						require.Equal(t, []Document{*testDocument}, docs)
						return nil, nil
					},
				}
			},
			msgs: []*msg{
				{write: testDocument, bytesSize: testSize},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(testSize), bytes)
					}
				},
			},

			wantErr: context.Canceled,
		},
		{
			name: "ok - schema change",
			store: func(doneChan chan struct{}) *mockStore {
				once := sync.Once{}
				return &mockStore{
					applySchemaChangeFn: func(ctx context.Context, le *schemalog.LogEntry) error {
						defer once.Do(func() { doneChan <- struct{}{} })
						require.Equal(t, testSchemaLogEntry, le)
						return nil
					},
				}
			},
			msgs: []*msg{
				{schemaChange: testSchemaLogEntry, bytesSize: testSize},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(testSize), bytes)
					}
				},
			},

			wantErr: context.Canceled,
		},
		{
			name: "error - sending batch",
			store: func(doneChan chan struct{}) *mockStore {
				once := sync.Once{}
				return &mockStore{
					sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						return nil, errTest
					},
				}
			},
			msgs: []*msg{
				{write: testDocument, bytesSize: testSize},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(testSize), bytes)
					}
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			indexer := &BatchIndexer{
				msgChan:           make(chan *msg, 100),
				store:             tc.store(doneChan),
				batchSendInterval: 100 * time.Millisecond,
				batchSize:         10,
				skipSchema:        func(schemaName string) bool { return false },
				queueBytesSema: &syncmocks.WeightedSemaphore{
					ReleaseFn: func(_ uint64, _ int64) {},
				},
			}

			if tc.semaphore != nil {
				indexer.queueBytesSema = tc.semaphore
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := indexer.Send(ctx)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			for _, msg := range tc.msgs {
				indexer.msgChan <- msg
			}

			for {
				select {
				case <-ctx.Done():
					t.Log("test timeout reached")
					wg.Wait()
					return
				case <-doneChan:
					if errors.Is(tc.wantErr, context.Canceled) {
						cancel()
					}
					wg.Wait()
					return
				}
			}
		})
	}
}

func TestBatchIndexer_sendBatch(t *testing.T) {
	t.Parallel()

	id := xid.New()
	now := time.Now()
	testLogEntry := newTestLogEntry(id, now)
	testCommitPos := newTestCommitPosition()
	testDocument1 := newTestDocument(withID("1"))
	testDocument2 := newTestDocument(withID("2"))

	tests := []struct {
		name       string
		store      Store
		checkpoint checkpoint
		batch      *msgBatch
		skipSchema func(string) bool
		cleaner    cleaner

		wantErr error
	}{
		{
			name:  "ok - no items in batch",
			batch: &msgBatch{},

			wantErr: nil,
		},
		{
			name: "ok - write only batch",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
					require.Equal(t, []Document{*testDocument1, *testDocument2}, docs)
					return nil, nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - write and schema change batch",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{schemaChange: testLogEntry},
					{write: testDocument2},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					switch i {
					case 1:
						require.Equal(t, []Document{*testDocument1}, docs)
						return nil, nil
					case 2:
						require.Equal(t, []Document{*testDocument2}, docs)
						return nil, nil
					}
					return nil, fmt.Errorf("sendDocumentsFn: unexpected call %d", i)
				},
				applySchemaChangeFn: func(ctx context.Context, le *schemalog.LogEntry) error {
					require.Equal(t, testLogEntry, le)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - write and truncate batch",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{truncate: &truncateItem{schemaName: testSchemaName, tableID: testTableName}},
					{write: testDocument2},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					switch i {
					case 1:
						require.Equal(t, []Document{*testDocument1}, docs)
						return nil, nil
					case 2:
						require.Equal(t, []Document{*testDocument2}, docs)
						return nil, nil
					}
					return nil, fmt.Errorf("sendDocumentsFn: unexpected call %d", i)
				},
				deleteTableDocumentsFn: func(ctx context.Context, schemaName string, tableIDs []string) error {
					require.Equal(t, testSchemaName, schemaName)
					require.Equal(t, []string{testTableName}, tableIDs)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - schema change skipped",
			batch: &msgBatch{
				msgs: []*msg{
					{schemaChange: testLogEntry},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store:      &mockStore{},
			skipSchema: func(s string) bool { return true },

			wantErr: nil,
		},
		{
			name: "ok - schema dropped",
			batch: &msgBatch{
				msgs: []*msg{
					{
						schemaChange: func() *schemalog.LogEntry {
							le := newTestLogEntry(id, now)
							le.Schema.Dropped = true
							return le
						}(),
					},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{},
			cleaner: &mockCleaner{
				deleteSchemaFn: func(ctx context.Context, s string) error {
					require.Equal(t, testSchemaName, s)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - applying schema change",
			batch: &msgBatch{
				msgs: []*msg{
					{schemaChange: testLogEntry},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				applySchemaChangeFn: func(ctx context.Context, le *schemalog.LogEntry) error {
					return errTest
				},
			},

			wantErr: nil,
		},
		{
			name: "error - applying schema delete",
			batch: &msgBatch{
				msgs: []*msg{
					{
						schemaChange: func() *schemalog.LogEntry {
							le := newTestLogEntry(id, now)
							le.Schema.Dropped = true
							return le
						}(),
					},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{},
			cleaner: &mockCleaner{
				deleteSchemaFn: func(ctx context.Context, s string) error {
					return errTest
				},
			},

			wantErr: nil,
		},
		{
			name: "error - flushing writes before truncate",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{truncate: &truncateItem{schemaName: testSchemaName, tableID: testTableName}},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - flushing writes before schema change",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{schemaChange: testLogEntry},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - truncating table",
			batch: &msgBatch{
				msgs: []*msg{
					{truncate: &truncateItem{schemaName: testSchemaName, tableID: testTableName}},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				deleteTableDocumentsFn: func(ctx context.Context, schemaName string, tableIDs []string) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - sending documents",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - checkpointing",
			batch: &msgBatch{
				msgs: []*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				positions: []wal.CommitPosition{testCommitPos},
			},
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
					return nil, nil
				},
			},
			checkpoint: func(ctx context.Context, positions []wal.CommitPosition) error {
				require.Equal(t, []wal.CommitPosition{testCommitPos}, positions)
				return errTest
			},

			wantErr: errTest,
		},
		{
			name: "error - empty queue item",
			batch: &msgBatch{
				msgs: []*msg{{}},
			},

			wantErr: errEmptyQueueMsg,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			indexer := &BatchIndexer{
				store:      tc.store,
				skipSchema: func(schemaName string) bool { return false },
			}
			indexer.SetCheckpoint(tc.checkpoint)

			if tc.skipSchema != nil {
				indexer.skipSchema = tc.skipSchema
			}

			if tc.cleaner != nil {
				indexer.cleaner = tc.cleaner
			}

			err := indexer.sendBatch(context.Background(), tc.batch)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
