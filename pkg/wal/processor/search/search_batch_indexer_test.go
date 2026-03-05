// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	batchmocks "github.com/xataio/pgstream/pkg/wal/processor/batch/mocks"

	"github.com/stretchr/testify/require"
)

func TestBatchIndexer_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testCommitPos := newTestCommitPosition()

	tests := []struct {
		name        string
		adapter     *mockAdapter
		event       *wal.Event
		batchSender *batchmocks.BatchSender[*msg]

		wantMsgs []*batch.WALMessage[*msg]
		wantErr  error
	}{
		{
			name: "ok - ddl event",
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return &msg{
						schemaDiff: newTestSchemaDiff(),
					}, nil
				},
			},
			event:       newTestWALDDLEvent(),
			batchSender: batchmocks.NewBatchSender[*msg](),

			wantMsgs: []*batch.WALMessage[*msg]{
				batch.NewWALMessage(
					&msg{
						schemaDiff: newTestSchemaDiff(),
					}, testCommitPos),
			},
			wantErr: nil,
		},
		{
			name: "ok - keep alive",
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return &msg{}, nil
				},
			},
			event: &wal.Event{
				CommitPosition: testCommitPos,
			},
			batchSender: batchmocks.NewBatchSender[*msg](),

			wantMsgs: []*batch.WALMessage[*msg]{
				batch.NewWALMessage(&msg{}, testCommitPos),
			},
			wantErr: nil,
		},
		{
			name: "ok - nil queue item",
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return nil, nil
				},
			},
			event:       newTestDataEvent("I"),
			batchSender: batchmocks.NewBatchSender[*msg](),

			wantMsgs: []*batch.WALMessage[*msg]{},
			wantErr:  nil,
		},
		{
			name: "ok - invalid wal event",
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return nil, errNilIDValue
				},
			},
			event:       newTestWALDDLEvent(),
			batchSender: batchmocks.NewBatchSender[*msg](),

			wantMsgs: []*batch.WALMessage[*msg]{},
			wantErr:  nil,
		},
		{
			name: "error - wal data to queue item",
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					return nil, errTest
				},
			},
			event:       newTestWALDDLEvent(),
			batchSender: batchmocks.NewBatchSender[*msg](),

			wantMsgs: []*batch.WALMessage[*msg]{},
			wantErr:  errTest,
		},
		{
			name: "error - panic recovery",
			adapter: &mockAdapter{
				walEventToMsgFn: func(*wal.Event) (*msg, error) {
					panic(errTest)
				},
			},
			event:       newTestDataEvent("I"),
			batchSender: batchmocks.NewBatchSender[*msg](),

			wantMsgs: []*batch.WALMessage[*msg]{},
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			indexer := BatchIndexer{
				logger:      loglib.NewNoopLogger(),
				batchSender: tc.batchSender,
				adapter:     tc.adapter,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				defer indexer.batchSender.Close()
				err := indexer.ProcessWALEvent(ctx, tc.event)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			msgs := tc.batchSender.GetWALMessages()
			require.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestBatchIndexer_sendBatch(t *testing.T) {
	t.Parallel()

	testCommitPos := newTestCommitPosition()
	testDocument1 := newTestDocument(withID("1"))
	testDocument2 := newTestDocument(withID("2"))

	tests := []struct {
		name       string
		store      Store
		checkpoint checkpointer.Checkpoint
		batch      *batch.Batch[*msg]
		skipSchema func(string) bool

		wantErr error
	}{
		{
			name:  "ok - no items in batch",
			batch: &batch.Batch[*msg]{},

			wantErr: nil,
		},
		{
			name: "ok - write only batch",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				[]wal.CommitPosition{testCommitPos}),

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
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{schemaDiff: newTestSchemaDiff()},
					{write: testDocument2},
				},
				[]wal.CommitPosition{testCommitPos}),

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
				applySchemaDiffFn: func(ctx context.Context, diff *wal.SchemaDiff) error {
					require.Equal(t, newTestSchemaDiff(), diff)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - write and truncate batch",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{truncate: &truncateItem{schemaName: testSchemaName, tableID: testTableName}},
					{write: testDocument2},
				},
				[]wal.CommitPosition{testCommitPos}),

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
			batch: batch.NewBatch(
				[]*msg{
					{schemaDiff: newTestSchemaDiff()},
				},
				[]wal.CommitPosition{testCommitPos}),

			store:      &mockStore{},
			skipSchema: func(s string) bool { return true },

			wantErr: nil,
		},
		{
			name: "ok - schema dropped",
			batch: batch.NewBatch(
				[]*msg{
					{
						schemaDiff: func() *wal.SchemaDiff {
							diff := newTestSchemaDiff()
							diff.SchemaDropped = true
							return diff
						}(),
					},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, _ uint, s string) error {
					require.Equal(t, testSchemaName, s)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - applying schema change",
			batch: batch.NewBatch(
				[]*msg{
					{schemaDiff: newTestSchemaDiff()},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				applySchemaDiffFn: func(ctx context.Context, diff *wal.SchemaDiff) error {
					return errTest
				},
			},

			wantErr: nil,
		},
		{
			name: "error - applying schema delete",
			batch: batch.NewBatch(
				[]*msg{
					{
						schemaDiff: func() *wal.SchemaDiff {
							diff := newTestSchemaDiff()
							diff.SchemaDropped = true
							return diff
						}(),
					},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, _ uint, s string) error {
					return errTest
				},
			},

			wantErr: nil,
		},
		{
			name: "error - flushing writes before truncate",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{truncate: &truncateItem{schemaName: testSchemaName, tableID: testTableName}},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - flushing writes before schema change",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{schemaDiff: newTestSchemaDiff()},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - truncating table",
			batch: batch.NewBatch(
				[]*msg{
					{truncate: &truncateItem{schemaName: testSchemaName, tableID: testTableName}},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				deleteTableDocumentsFn: func(ctx context.Context, schemaName string, tableIDs []string) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - sending documents",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - sending documents with validation failure",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				[]wal.CommitPosition{testCommitPos}),

			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, _ uint, docs []Document) ([]DocumentError, error) {
					return nil, ErrInvalidQuery
				},
			},

			wantErr: nil,
		},
		{
			name: "error - checkpointing",
			batch: batch.NewBatch(
				[]*msg{
					{write: testDocument1},
					{write: testDocument2},
				},
				[]wal.CommitPosition{testCommitPos}),

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
			batch: batch.NewBatch(
				[]*msg{{}},
				[]wal.CommitPosition{testCommitPos}),

			wantErr: errEmptyQueueMsg,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			indexer := &BatchIndexer{
				logger:     loglib.NewNoopLogger(),
				store:      tc.store,
				skipSchema: func(schemaName string) bool { return false },
				checkpoint: tc.checkpoint,
			}

			if tc.skipSchema != nil {
				indexer.skipSchema = tc.skipSchema
			}

			err := indexer.sendBatch(context.Background(), tc.batch)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
