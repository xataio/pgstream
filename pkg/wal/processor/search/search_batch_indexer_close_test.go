// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// TestBatchIndexer_Close_reportsDropsFromTheFinalDrain pins the ordering inside
// Close. The sender drains its in-flight batch as it closes, so a batch dropped
// there is only counted once the sender has returned: logging the totals first
// reports a stale number, and for an indexer that only ever dropped on shutdown
// it reports nothing at all.
func TestBatchIndexer_Close_reportsDropsFromTheFinalDrain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dropped := batch.NewDroppedCounter()
	logger := newRecordingLogger()

	// the send always fails and errors are ignored, so the batch the drain
	// picks up is dropped rather than indexed
	sender, err := batch.NewSender(ctx, &batch.Config{
		IgnoreSendErrors: true,
		MaxBatchSize:     100,
		// only the drain on close sends this batch, never the ticker
		BatchTimeout: time.Hour,
	}, func(context.Context, *batch.Batch[*msg]) error {
		return errTest
	}, loglib.NewNoopLogger(), batch.WithDroppedCounter[*msg](dropped))
	require.NoError(t, err)

	indexer := &BatchIndexer{
		logger:      logger,
		batchSender: sender,
		dropped:     dropped,
	}

	require.NoError(t, sender.SendMessage(ctx, batch.NewWALMessage(
		&msg{schemaDiff: newTestSchemaDiff(), bytesSize: 1}, newTestCommitPosition())))

	require.NoError(t, indexer.Close())

	require.Equal(t, uint64(1), dropped.Batches(), "the drain on close should have dropped the batch")

	fields := logger.find(droppedTotalsLogMsg)
	require.NotNil(t, fields, "the shutdown summary was not logged")
	require.Equal(t, uint64(1), fields["dropped_batches"])
	require.Equal(t, uint64(1), fields["dropped_messages"])
}

const droppedTotalsLogMsg = "closed with dropped batches"

// recordingLogger keeps the fields of the messages logged at error level, so a
// test can assert on what a shutdown summary reported.
type recordingLogger struct {
	*loglib.NoopLogger

	mutex   sync.Mutex
	entries map[string]loglib.Fields
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{
		NoopLogger: loglib.NewNoopLogger(),
		entries:    map[string]loglib.Fields{},
	}
}

func (l *recordingLogger) Error(_ error, msg string, fields ...loglib.Fields) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	merged := loglib.Fields{}
	for _, f := range fields {
		merged = loglib.MergeFields(merged, f)
	}
	l.entries[msg] = merged
}

func (l *recordingLogger) find(msg string) loglib.Fields {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.entries[msg]
}
