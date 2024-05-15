// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/internal/replication"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type BatchKafkaWriter struct {
	writer kafkaWriter

	// queueBytesSema is used to limit the amount of memory used by the
	// unbuffered msg channel, optimising the channel performance for variable
	// size messages, while preventing the process from running oom
	queueBytesSema weightedSemaphore
	msgChan        chan (*kafkaMsg)

	maxBatchBytes int64
	maxBatchSize  int
	sendFrequency time.Duration

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpoint

	eventSerialiser func(any) ([]byte, error)
}

type kafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type weightedSemaphore interface {
	TryAcquire(int64) bool
	Acquire(context.Context, int64) error
	Release(int64)
}

// checkpoint defines the way to confirm the positions that have been read.
// The actual implementation depends on the source of events (postgres, kafka,...)
type checkpoint func(ctx context.Context, positions []commitPosition) error

// commitPosition represents a position in the input stream, which can be either
// postgres or kafka
type commitPosition struct {
	pgPos replication.LSN
}

const defaultMaxQueueBytes = 100 * 1024 * 1024 // 100MiB

func NewBatchKafkaWriter(config kafka.WriterConfig) (*BatchKafkaWriter, error) {
	w := &BatchKafkaWriter{
		sendFrequency:   config.BatchTimeout,
		maxBatchBytes:   config.BatchBytes,
		maxBatchSize:    config.BatchSize,
		msgChan:         make(chan *kafkaMsg),
		eventSerialiser: json.Marshal,
	}

	maxQueueBytes := defaultMaxQueueBytes
	if config.MaxQueueBytes > 0 {
		if config.MaxQueueBytes < config.BatchBytes {
			return nil, errors.New("max queue bytes must be equal or bigger than the batch bytes")
		}
		maxQueueBytes = int(config.MaxQueueBytes)
	}
	w.queueBytesSema = semaphore.NewWeighted(int64(maxQueueBytes))

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
	const batchTimeout = 10 * time.Millisecond
	config.BatchTimeout = batchTimeout
	var err error
	w.writer, err = kafka.NewWriter(config)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// ProcessWalEvent is called on every new message from the wal
func (w *BatchKafkaWriter) ProcessWALEvent(ctx context.Context, walEvent *wal.Data, pos commitPosition) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			log.Ctx(ctx).WithLevel(zerolog.PanicLevel).
				Any("wal_data", walEvent).
				Any("panic", r).
				Bytes("stack_trace", debug.Stack()).
				Msg("[PANIC] Panic while processing replication event")

			retErr = fmt.Errorf("kafka batch writer: understanding event: %v", r)
		}
	}()

	walEventBytes, err := w.eventSerialiser(walEvent)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}
	// check if walEventBytes is larger than the Kafka accepted max message size
	if len(walEventBytes) > int(w.maxBatchBytes) {
		log.Ctx(ctx).Warn().
			Str("warning", "record too large").
			Int("size", len(walEventBytes)).
			Str("table", walEvent.Table).
			Str("schema", walEvent.Schema).
			Msgf("kafka batch writer: record wal event is larger than %d bytes", w.maxBatchBytes)
		return nil
	}

	kafkaMsg := &kafkaMsg{
		msg: kafka.Message{
			Key:   w.getMessageKey(walEvent),
			Value: walEventBytes,
		},
		pos: pos.pgPos,
	}

	// make sure we don't reach the queue memory limit before adding the new
	// message to the channel. This will block until messages have been read
	// from the channel and their size is released
	msgSize := int64(kafkaMsg.size())
	if !w.queueBytesSema.TryAcquire(msgSize) {
		log.Ctx(ctx).Warn().Msg("kafka batch writer: max queue bytes reached, processing blocked")
		if err := w.queueBytesSema.Acquire(ctx, msgSize); err != nil {
			return err
		}
	}

	w.msgChan <- kafkaMsg

	return nil
}

func (w *BatchKafkaWriter) SendThread(ctx context.Context) error {
	// make sure we send to kafka on a separate go routine to isolate the IO
	// operations, ensuring the kafka goroutine is always sending, and minimise
	// the wait time between batch sending
	batchChan := make(chan *kafkaMsgBatch)
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
	msgBatch := &kafkaMsgBatch{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sendErr := <-sendErrChan:
			// if there's an error while sending the batch, return the error and
			// stop sending batches
			return sendErr
		case <-ticker.C:
			batchChan <- msgBatch.drain()
		case msg := <-w.msgChan:
			// if the batch has reached the max allowed size, don't wait for the
			// next tick and send to kafka
			if msgBatch.totalBytes+msg.size() >= int(w.maxBatchBytes) ||
				len(msgBatch.msgs) == w.maxBatchSize {
				batchChan <- msgBatch.drain()
			}

			msgBatch.add(msg)
		}
	}
}

func (w *BatchKafkaWriter) Close() error {
	close(w.msgChan)
	return w.writer.Close()
}

func (w *BatchKafkaWriter) SetCheckpoint(checkpoint checkpoint) {
	w.checkpointer = checkpoint
}

func (w *BatchKafkaWriter) sendBatch(ctx context.Context, batch *kafkaMsgBatch) error {
	if len(batch.msgs) == 0 {
		return nil
	}

	log.Ctx(ctx).Debug().Msgf("kafka batch writer: sending message batch size %d with pos: %X", len(batch.msgs), batch.lastPos)

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
		log.Ctx(ctx).Error().Err(err).Msg("failed to write to kafka")
		return fmt.Errorf("kafka batch writer: writing to kafka: %w", err)
	}

	if w.checkpointer != nil {
		if err := w.checkpointer(ctx, []commitPosition{{pgPos: batch.lastPos}}); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("kafka batch writer: error updating commit position")
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
func (w BatchKafkaWriter) getMessageKey(walEvent *wal.Data) []byte {
	eventKey := walEvent.Schema
	if isSchemaLogEvent(walEvent) {
		var schemaName string
		var found bool
		for _, col := range walEvent.Columns {
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

func isSchemaLogEvent(e *wal.Data) bool {
	return e.Schema == schemalog.SchemaName && e.Table == schemalog.TableName
}
