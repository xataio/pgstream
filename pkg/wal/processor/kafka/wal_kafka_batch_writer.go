// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkainstrumentation "github.com/xataio/pgstream/pkg/kafka/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a kafka writer that uses batches to send the data to the
// configured kafka topic.
type BatchWriter struct {
	writer        kafka.MessageWriter
	logger        loglib.Logger
	batchSender   batchSender
	maxBatchBytes int64
	partitionKey  PartitionKey

	// dropped accumulates what ignore_send_errors has discarded, and is shared
	// with the sender so the totals and metrics cover this writer.
	dropped         *batch.DroppedCounter
	instrumentation *otel.Instrumentation

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpointer.Checkpoint

	serialiser        func(any) ([]byte, error)
	walDataToDDLEvent func(*wal.Data) (*wal.DDLEvent, error)
}

type Option func(*BatchWriter)

type batchSender interface {
	SendMessage(context.Context, *batch.WALMessage[kafka.Message]) error
	Close() error
}

var errRecordTooLarge = errors.New("record too large")

func NewBatchWriter(ctx context.Context, config *Config, opts ...Option) (*BatchWriter, error) {
	partitionKey, err := config.partitionKey()
	if err != nil {
		return nil, err
	}

	w := &BatchWriter{
		serialiser:        json.Marshal,
		logger:            loglib.NewNoopLogger(),
		maxBatchBytes:     config.Batch.GetMaxBatchBytes(),
		partitionKey:      partitionKey,
		walDataToDDLEvent: wal.WalDataToDDLEvent,
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
		BatchSize:    int(config.Batch.GetMaxBatchSize()),
		BatchBytes:   config.Batch.GetMaxBatchBytes(),
	}, w.logger)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(w)
	}

	w.dropped = batch.NewDroppedCounter()
	if config.Batch.IgnoreSendErrors {
		w.logger.Warn(nil, "ignore_send_errors is enabled: batches that fail to send will be dropped and the run will continue", loglib.Fields{
			"posture":     "at_risk",
			"writer_type": kafkaWriterType,
		})
	}
	if err := w.dropped.RegisterMetrics(w.instrumentation, kafkaWriterType); err != nil {
		return nil, fmt.Errorf("initialising kafka batch writer metrics: %w", err)
	}

	w.batchSender, err = batch.NewSender(ctx, &config.Batch, w.sendBatch, w.logger,
		batch.WithDroppedCounter[kafka.Message](w.dropped))
	if err != nil {
		return nil, err
	}

	return w, nil
}

const kafkaWriterType = "kafka_batch_writer"

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
		w.instrumentation = i
		instrumentedWriter, err := kafkainstrumentation.NewWriter(w.writer, i)
		if err != nil {
			w.logger.Error(err, "initialising kafka writer instrumentation")
			return
		}
		w.writer = instrumentedWriter
	}
}

// ProcessWALEvent is called on every new message from the wal. It can be called
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
		// the key counts towards the record size the broker enforces, and it
		// carries row data (primary key values), so it has to be measured with
		// the value rather than after the check
		key := w.getMessageKey(walEvent.Data)

		// check if the record is larger than 95% of the Kafka accepted max
		// message size to allow for some buffer for the rest of the message
		if len(walDataBytes)+len(key) > int(0.95*float64(w.maxBatchBytes)) {
			w.logger.Warn(errRecordTooLarge,
				"kafka batch writer: wal event is larger than 95% of max bytes allowed",
				loglib.Fields{
					"max_bytes": w.maxBatchBytes,
					"size":      len(walDataBytes) + len(key),
					"key_size":  len(key),
					"table":     walEvent.Data.Table,
					"schema":    walEvent.Data.Schema,
				})
			return nil
		}

		kafkaMsg = kafka.Message{
			Key:   key,
			Value: walDataBytes,
		}
	}

	msg := batch.NewWALMessage(kafkaMsg, walEvent.CommitPosition)
	return w.batchSender.SendMessage(ctx, msg)
}

func (w *BatchWriter) Name() string {
	return "kafka-batch-writer"
}

func (w *BatchWriter) Close() error {
	err := errors.Join(w.batchSender.Close(), w.writer.Close())
	w.dropped.LogTotals(w.logger)
	return err
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
// and therefore which order the events will be executed in. For DDL events, we
// extract the underlying user schema they're linked to in the content, to make
// sure they're routed to the same partition as their schema keyed writes. DML
// events are keyed following the configured partition key strategy, which
// defaults to the schema name (ordering per schema).
func (w BatchWriter) getMessageKey(walData *wal.Data) []byte {
	if walData.IsDDLEvent() {
		ddlEvent, err := w.walDataToDDLEvent(walData)
		if err != nil {
			w.logger.Error(err, "parsing ddl event for schema", loglib.Fields{
				"wal_data": walData,
			})
			return []byte(walData.Schema)
		}
		return []byte(ddlEvent.SchemaName)
	}

	switch w.partitionKey {
	case PartitionKeyPrimaryKey:
		if key := primaryKeyMessageKey(walData); key != nil {
			return key
		}
		// no identifiable primary key for the event (no injector configured,
		// or table without primary key), degrade gracefully to table keying
		return tableMessageKey(walData)
	case PartitionKeyTable:
		return tableMessageKey(walData)
	default:
		return []byte(walData.Schema)
	}
}

// The message key encodes a row identity as
//
//	<schema>.<table>:<value>,<value>,...
//
// Each component is escaped so that the delimiters can only appear as
// structure, never as data. Without it the encoding is not injective and
// distinct rows share a message key: ["a,b","c"] and ["a","b,c"] both render as
// "a,b,c", and schema "a.b"+table "c" renders the same as schema "a"+table
// "b.c" — colliding rows lose their separate partitioning identity, and on a
// compacted topic one silently shadows the other.
//
// Identifiers and values use different escape sets on purpose. Only the
// delimiters that actually terminate a component need escaping there, and every
// byte escaped is a message key that moves to a different partition on upgrade,
// so the sets are kept as small as correctness allows. Values may contain "."
// and ":" freely, since the value list is everything after the first unescaped
// ":"; identifiers may contain "," freely for the same reason.
//
// Both replacers are logically constant and safe for concurrent use.
var (
	keyValueEscaper      = strings.NewReplacer(`\`, `\\`, `,`, `\,`)
	keyIdentifierEscaper = strings.NewReplacer(`\`, `\\`, `.`, `\.`, `:`, `\:`)
)

// primaryKeyMessageKey returns a key composed of the schema qualified table
// name and the values of the event primary key columns, as identified by the
// injector in the wal metadata. Identifiers and values are escaped so that
// distinct rows never share a key. It returns nil if the primary key columns or
// their values can't be found in the event.
func primaryKeyMessageKey(walData *wal.Data) []byte {
	colIDs := walData.Metadata.InternalColIDs
	if len(colIDs) == 0 {
		return nil
	}

	values := make([]string, 0, len(colIDs))
	for _, colID := range colIDs {
		col, found := findColumn(walData.Columns, colID)
		if !found {
			// delete events don't include the new row values, rely on the
			// identity (old) values instead
			col, found = findColumn(walData.Identity, colID)
		}
		if !found {
			return nil
		}
		values = append(values, keyValueEscaper.Replace(fmt.Sprintf("%v", col.Value)))
	}

	return fmt.Appendf(tableMessageKey(walData), ":%s", strings.Join(values, ","))
}

// tableMessageKey returns the schema qualified table name, with both
// identifiers escaped so that the "." separating them is unambiguous —
// postgres quoted identifiers may contain it.
func tableMessageKey(walData *wal.Data) []byte {
	return []byte(keyIdentifierEscaper.Replace(walData.Schema) + "." +
		keyIdentifierEscaper.Replace(walData.Table))
}

func findColumn(cols []wal.Column, id string) (wal.Column, bool) {
	for _, col := range cols {
		if col.ID == id {
			return col, true
		}
	}
	return wal.Column{}, false
}
