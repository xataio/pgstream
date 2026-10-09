// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// Conversion failures must still reach the caller and prevent checkpointing
// now that conversion runs on the ordered sender rather than at ingestion.
func TestBatchWriter_sendBatch_preparationFailure(t *testing.T) {
	t.Parallel()
	for _, panicOnPrepare := range []bool{false, true} {
		name := "conversion error"
		wantErr := errTest
		if panicOnPrepare {
			name, wantErr = "conversion panic", processor.ErrPanic
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			calls, writes, checkpoints := 0, 0, 0
			writer := &BatchWriter{Writer: &Writer{
				logger: loglib.NewNoopLogger(),
				adapter: &mockAdapter{walEventToMessageFn: func(*wal.Event) (*walMessage, error) {
					calls++
					if panicOnPrepare {
						panic(errTest)
					}
					return nil, errTest
				}},
				pgConn: &pgmocks.Querier{ExecInTxFn: func(context.Context, func(pglib.Tx) error) error {
					writes++
					return nil
				}},
				checkpointer: func(context.Context, []wal.CommitPosition) error {
					checkpoints++
					return nil
				},
			}}
			queued := &walMessage{data: &wal.Data{Action: "I", Schema: testSchema, Table: testTable}, needsPreparation: true}
			b := batch.NewBatch([]*walMessage{queued}, []wal.CommitPosition{testCommitPosition})
			require.ErrorIs(t, writer.sendBatch(context.Background(), b), wantErr)
			require.Equal(t, 1, calls)
			require.Zero(t, writes)
			require.Zero(t, checkpoints)
			require.True(t, queued.needsPreparation, "failed preparation must not mutate the queued event")
		})
	}
}

func TestBatchWriter_sendBatch_preparationSkipsFilteredEvent(t *testing.T) {
	t.Parallel()
	checkpoints := 0
	writer := &BatchWriter{Writer: &Writer{
		logger: loglib.NewNoopLogger(),
		adapter: &mockAdapter{walEventToMessageFn: func(*wal.Event) (*walMessage, error) {
			return &walMessage{}, nil
		}},
		checkpointer: func(_ context.Context, positions []wal.CommitPosition) error {
			checkpoints++
			require.Equal(t, []wal.CommitPosition{testCommitPosition}, positions)
			return nil
		},
	}}
	b := batch.NewBatch([]*walMessage{{data: &wal.Data{Action: "I"}, needsPreparation: true}}, []wal.CommitPosition{testCommitPosition})
	require.NoError(t, writer.sendBatch(context.Background(), b))
	require.Equal(t, 1, checkpoints, "intentionally skipped events still advance the checkpoint")
}
