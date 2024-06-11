// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/xataio/pgstream/internal/kafka"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type BatchWriter struct {
	writer kafkaWriter
	logger loglib.Logger

	// queueBytesSema is used to limit the amount of memory used by the
	// unbuffered msg channel, optimising the channel performance for variable
	// size messages, while preventing the process from running oom
	queueBytesSema synclib.WeightedSemaphore
	msgChan        chan (*msg)

	maxBatchBytes int64
	maxBatchSize  int
	sendFrequency time.Duration

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpointer.Checkpoint

	serialiser func(any) ([]byte, error)
}

type kafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Option func(*BatchWriter)

var errRecordTooLarge = errors.New("record too large")

func NewBatchWriter(config *Config, opts ...Option) (*BatchWriter, error) {
	w := &BatchWriter{
		sendFrequency: config.batchTimeout(),
		maxBatchBytes: config.batchBytes(),
		maxBatchSize:  config.batchSize(),
		msgChan:       make(chan *msg),
		serialiser:    json.Marshal,
		logger:        loglib.NewNoopLogger(),
	}

	maxQueueBytes, err := config.maxQueueBytes()
	if err != nil {
		return nil, err
	}
	w.queueBytesSema = synclib.NewWeightedSemaphore(int64(maxQueueBytes))

	for _, opt := range opts {
		opt(w)
	}

	// Since the batch kafka writer handles the batching, we don't want to have
	// a timeout configured in the underlying kafka-go writer or the latency for
	// the send will increase unnecessarily. Instead, we set the kafka-go writer
	// batch timeout to a low value so that it triggers the writes as soon as we
	// send the batch.
	//
	// While we could use a connection instead of the writer to avoid the
	// batching behaviour of the kafka-go library, the writer adds handling for
	// additional features (automatic retries, reconnection, distribution of
	// messages across partitions,etc) which we want to benefit from.
	const kafkaBatchTimeout = 10 * time.Millisecond
	w.writer, err = kafka.NewWriter(kafka.WriterConfig{
		Conn:         config.Kafka,
		BatchTimeout: kafkaBatchTimeout,
		BatchSize:    config.batchSize(),
		BatchBytes:   config.batchBytes(),
	}, w.logger)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(w *BatchWriter) {
		w.logger = loglib.NewLogger(l)
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(w *BatchWriter) {
		w.checkpointer = c
	}
}

// ProcessWalEvent is called on every new message from the wal
func (w *BatchWriter) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Panic("[PANIC] Panic while processing replication event", loglib.Fields{
				"wal_data":    walEvent,
				"panic":       r,
				"stack_trace": debug.Stack(),
			})

			retErr = fmt.Errorf("kafka batch writer: understanding event: %v", r)
		}
	}()

	kafkaMsg := &msg{
		pos: walEvent.CommitPosition,
	}

	if walEvent.Data != nil {
		walDataBytes, err := w.serialiser(walEvent.Data)
		if err != nil {
			return fmt.Errorf("marshalling event: %w", err)
		}
		// check if walEventBytes is larger than the Kafka accepted max message size
		if len(walDataBytes) > int(w.maxBatchBytes) {
			w.logger.Warn(errRecordTooLarge,
				"kafka batch writer: wal event is larger than max bytes",
				loglib.Fields{
					"max_bytes": w.maxBatchBytes,
					"size":      len(walDataBytes),
					"table":     walEvent.Data.Table,
					"schema":    walEvent.Data.Schema,
				})
			return nil
		}

		kafkaMsg.msg = kafka.Message{
			Key:   w.getMessageKey(walEvent.Data),
			Value: walDataBytes,
		}
	}

	// make sure we don't reach the queue memory limit before adding the new
	// message to the channel. This will block until messages have been read
	// from the channel and their size is released
	msgSize := int64(kafkaMsg.size())
	if !w.queueBytesSema.TryAcquire(msgSize) {
		w.logger.Warn(nil, "kafka batch writer: max queue bytes reached, processing blocked")
		if err := w.queueBytesSema.Acquire(ctx, msgSize); err != nil {
			return err
		}
	}

	w.msgChan <- kafkaMsg

	return nil
}

func (w *BatchWriter) Send(ctx context.Context) error {
	// make sure we send to kafka on a separate go routine to isolate the IO
	// operations, ensuring the kafka goroutine is always sending, and minimise
	// the wait time between batch sending
	batchChan := make(chan *msgBatch)
	defer close(batchChan)
	sendErrChan := make(chan error, 1)
	go func() {
		defer close(sendErrChan)
		for batch := range batchChan {
			// If the send fails, the writer goroutine returns an error over the error channel and shuts down.
			err := w.sendBatch(ctx, batch)
			w.queueBytesSema.Release(int64(batch.totalBytes))
			if err != nil {
				sendErrChan <- err
				return
			}
		}
	}()

	// we will send to kafka either as soon as the batch is full, or as soon as
	// the configured send frequency hits
	ticker := time.NewTicker(w.sendFrequency)
	defer ticker.Stop()
	msgBatch := &msgBatch{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sendErr := <-sendErrChan:
			// if there's an error while sending the batch, return the error and
			// stop sending batches
			return sendErr
		case <-ticker.C:
			if !msgBatch.isEmpty() {
				batchChan <- msgBatch.drain()
			}
		case msg := <-w.msgChan:
			// if the batch has reached the max allowed size, don't wait for the
			// next tick and send to kafka.
			if msgBatch.totalBytes+msg.size() >= int(w.maxBatchBytes) ||
				len(msgBatch.msgs) == w.maxBatchSize {
				batchChan <- msgBatch.drain()
			}

			msgBatch.add(msg)
			// If we receive a keep alive, send so that we checkpoint as soon as
			// possible.
			if msg.isKeepAlive() {
				batchChan <- msgBatch.drain()
			}
		}
	}
}

func (w *BatchWriter) Close() error {
	close(w.msgChan)
	return w.writer.Close()
}

func (w *BatchWriter) sendBatch(ctx context.Context, batch *msgBatch) error {
	w.logger.Debug("kafka batch writer: sending message batch", loglib.Fields{
		"batch_size":             len(batch.msgs),
		"batch_commit_positions": len(batch.positions),
	})

	if len(batch.msgs) > 0 {
		// This call will block until it either reaches the writer configured batch
		// size or the batch timeout. This batching feature is useful when sharing a
		// writer across multiple go routines. In our case, we only send from a
		// single go routine, so we use a low value for the batch timeout, and
		// trigger the send immediately while handling the batching on our end to
		// improve throughput and reduce send latency.
		//
		// We don't use an asynchronous writer since we need to know if the messages
		// fail to be written to kafka.
		if err := w.writer.WriteMessages(ctx, batch.msgs...); err != nil {
			w.logger.Error(err, "failed to write to kafka")
			return fmt.Errorf("kafka batch writer: writing to kafka: %w", err)
		}
	}

	if w.checkpointer != nil && len(batch.positions) > 0 {
		if err := w.checkpointer(ctx, batch.positions); err != nil {
			w.logger.Warn(err, "kafka batch writer: error updating commit position")
		}
	}

	return nil
}

// getMessageKey returns the key to be used in a kafka message for the wal event
// on input. The message key determines which partition the event is routed to,
// and therefore which order the events will be executed in. For schema logs,
// the event schema is that of the pgstream schema, so we extract the underlying
// user schema they're linked to, to make sure they're routed to the same
// partition as their writes. This gives us ordering per schema.
func (w BatchWriter) getMessageKey(walData *wal.Data) []byte {
	eventKey := walData.Schema
	if processor.IsSchemaLogEvent(walData) {
		var schemaName string
		var found bool
		for _, col := range walData.Columns {
			if col.Name == "schema_name" {
				var ok bool
				if schemaName, ok = col.Value.(string); !ok {
					// We've got schema name, but it's not a string. This would mean the schema_log has changed and
					// this code has not been updated.
					panic(fmt.Sprintf("schema_log schema_name received is not a string: %T", col.Value))
				}
				found = true
				break
			}
		}
		if !found {
			// this means the schema name has not been found in the columns written. This would mean that we've
			// received a schema_log event, but without enough columns to act on it. This indicates a schema
			// change that we've not handled.
			panic("schema_log schema_name not found in columns")
		}
		eventKey = schemaName
	}

	return []byte(eventKey)
}
