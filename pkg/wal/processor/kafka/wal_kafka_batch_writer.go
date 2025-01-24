// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkainstrumentation "github.com/xataio/pgstream/pkg/kafka/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a kafka writer that uses batches to send the data to the
// configured kafka topic.
type BatchWriter struct {
	writer        kafka.MessageWriter
	logger        loglib.Logger
	batchSender   batchSender
	maxBatchBytes int64

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpointer.Checkpoint

	serialiser func(any) ([]byte, error)
}

type Option func(*BatchWriter)

type batchSender interface {
	AddToBatch(context.Context, *batch.WALMessage[kafka.Message]) error
	Close()
	Send(context.Context) error
}

var errRecordTooLarge = errors.New("record too large")

func NewBatchWriter(ctx context.Context, config *Config, opts ...Option) (*BatchWriter, error) {
	w := &BatchWriter{
		serialiser:    json.Marshal,
		logger:        loglib.NewNoopLogger(),
		maxBatchBytes: config.Batch.GetMaxBatchBytes(),
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
	var err error
	w.writer, err = kafka.NewWriter(kafka.WriterConfig{
		Conn:         config.Kafka,
		BatchTimeout: kafkaBatchTimeout,
		BatchSize:    int(config.Batch.GetMaxBatchSize()),
		BatchBytes:   config.Batch.GetMaxBatchBytes(),
	}, w.logger)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(w)
	}

	w.batchSender, err = batch.NewSender(&config.Batch, w.sendBatch, w.logger)
	if err != nil {
		return nil, err
	}

	// start the send process in the background
	go func() {
		if err := w.batchSender.Send(ctx); err != nil {
			w.logger.Error(err, "sending stopped")
		}
	}()

	return w, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(w *BatchWriter) {
		w.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "kafka_batch_writer",
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(w *BatchWriter) {
		w.checkpointer = c
	}
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(w *BatchWriter) {
		instrumentedWriter, err := kafkainstrumentation.NewWriter(w.writer, i)
		if err != nil {
			w.logger.Error(err, "initialising kafka writer instrumentation")
			return
		}
		w.writer = instrumentedWriter
	}
}

// ProcessWalEvent is called on every new message from the wal. It can be called
// concurrently.
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

	kafkaMsg := kafka.Message{}
	if walEvent.Data != nil {
		walDataBytes, err := w.serialiser(walEvent.Data)
		if err != nil {
			return fmt.Errorf("marshalling event: %w", err)
		}
		// check if walEventBytes is larger than 95% of the Kafka accepted max
		// message size to allow for some buffer for the rest of the message
		if len(walDataBytes) > int(0.95*float64(w.maxBatchBytes)) {
			w.logger.Warn(errRecordTooLarge,
				"kafka batch writer: wal event is larger than 95% of max bytes allowed",
				loglib.Fields{
					"max_bytes": w.maxBatchBytes,
					"size":      len(walDataBytes),
					"table":     walEvent.Data.Table,
					"schema":    walEvent.Data.Schema,
				})
			return nil
		}

		kafkaMsg = kafka.Message{
			Key:   w.getMessageKey(walEvent.Data),
			Value: walDataBytes,
		}
	}

	msg := batch.NewWALMessage(kafkaMsg, walEvent.CommitPosition)
	return w.batchSender.AddToBatch(ctx, msg)
}

func (w *BatchWriter) Name() string {
	return "kafka-batch-writer"
}

func (w *BatchWriter) Close() error {
	w.batchSender.Close()
	return w.writer.Close()
}

func (w *BatchWriter) sendBatch(ctx context.Context, batch *batch.Batch[kafka.Message]) error {
	messages := batch.GetMessages()
	w.logger.Debug("kafka batch writer: sending message batch", loglib.Fields{
		"batch_size":             len(messages),
		"batch_commit_positions": len(batch.GetCommitPositions()),
	})

	if len(messages) > 0 {
		// This call will block until it either reaches the writer configured batch
		// size or the batch timeout. This batching feature is useful when sharing a
		// writer across multiple go routines. In our case, we only send from a
		// single go routine, so we use a low value for the batch timeout, and
		// trigger the send immediately while handling the batching on our end to
		// improve throughput and reduce send latency.
		//
		// We don't use an asynchronous writer since we need to know if the messages
		// fail to be written to kafka.
		if err := w.writer.WriteMessages(ctx, messages...); err != nil {
			w.logger.Error(err, "failed to write to kafka")
			return fmt.Errorf("kafka batch writer: writing to kafka: %w", err)
		}
	}

	positions := batch.GetCommitPositions()
	if w.checkpointer != nil && len(positions) > 0 {
		if err := w.checkpointer(ctx, positions); err != nil {
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
