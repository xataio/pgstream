// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkamocks "github.com/xataio/pgstream/pkg/kafka/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// TestBatchWriter_Close_reportsDropsFromTheFinalDrain pins the ordering inside
// Close. The sender drains its in-flight batch as it closes, so a batch dropped
// there is only counted once the sender has returned: logging the totals first
// reports a stale number, and for a writer that only ever dropped on shutdown
// it reports nothing at all.
func TestBatchWriter_Close_reportsDropsFromTheFinalDrain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dropped := batch.NewDroppedCounter()
	logger := newRecordingLogger()

	// the send always fails and errors are ignored, so the batch the drain
	// picks up is dropped rather than sent
	sender, err := batch.NewSender(ctx, &batch.Config{
		IgnoreSendErrors: true,
		MaxBatchSize:     100,
		// only the drain on close sends this batch, never the ticker
		BatchTimeout: time.Hour,
	}, func(context.Context, *batch.Batch[kafka.Message]) error {
		return errTest
	}, loglib.NewNoopLogger(), batch.WithDroppedCounter[kafka.Message](dropped))
	require.NoError(t, err)

	writer := &BatchWriter{
		logger:      logger,
		batchSender: sender,
		dropped:     dropped,
		writer:      &kafkamocks.Writer{CloseFn: func() error { return nil }},
	}

	require.NoError(t, sender.SendMessage(ctx, batch.NewWALMessage(
		kafka.Message{Value: []byte("test")}, wal.CommitPosition(testLSNStr))))

	require.NoError(t, writer.Close())

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
