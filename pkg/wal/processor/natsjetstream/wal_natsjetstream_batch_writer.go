// SPDX-License-Identifier: Apache-2.0

package natsjetstream

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/xataio/pgstream/internal/json"
	natslib "github.com/xataio/pgstream/pkg/nats"
	natsinstrumentation "github.com/xataio/pgstream/pkg/nats/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a NATS JetStream writer that uses batches to send the data to
// the configured NATS JetStream stream.
type BatchWriter struct {
	writer        natslib.MessageWriter
	logger        loglib.Logger
	batchSender   batchSender
	maxBatchBytes int64

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpointer.Checkpoint

	serialiser        func(any) ([]byte, error)
	walDataToDDLEvent func(*wal.Data) (*wal.DDLEvent, error)
}

type Option func(*BatchWriter)

type batchSender interface {
	SendMessage(context.Context, *batch.WALMessage[natslib.Message]) error
	Close()
}

var errRecordTooLarge = errors.New("record too large")

func NewBatchWriter(ctx context.Context, config *Config, opts ...Option) (*BatchWriter, error) {
	w := &BatchWriter{
		serialiser:        json.Marshal,
		logger:            loglib.NewNoopLogger(),
		maxBatchBytes:     config.Batch.GetMaxBatchBytes(),
		walDataToDDLEvent: wal.WalDataToDDLEvent,
	}

	var err error
	w.writer, err = natslib.NewWriter(natslib.WriterConfig{
		Conn: config.NATS,
	}, w.logger)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(w)
	}

	w.batchSender, err = batch.NewSender(ctx, &config.Batch, w.sendBatch, w.logger)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(w *BatchWriter) {
		w.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "nats_jetstream_batch_writer",
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
		instrumentedWriter, err := natsinstrumentation.NewWriter(w.writer, i)
		if err != nil {
			w.logger.Error(err, "initialising nats jetstream writer instrumentation")
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

			retErr = fmt.Errorf("nats jetstream batch writer: understanding event: %v", r)
		}
	}()

	natsMsg := natslib.Message{}
	if walEvent.Data != nil {
		walDataBytes, err := w.serialiser(walEvent.Data)
		if err != nil {
			return fmt.Errorf("marshalling event: %w", err)
		}
		// check if walEventBytes is larger than 95% of the accepted max
		// message size to allow for some buffer for the rest of the message
		if len(walDataBytes) > int(0.95*float64(w.maxBatchBytes)) {
			w.logger.Warn(errRecordTooLarge,
				"nats jetstream batch writer: wal event is larger than 95% of max bytes allowed",
				loglib.Fields{
					"max_bytes": w.maxBatchBytes,
					"size":      len(walDataBytes),
					"table":     walEvent.Data.Table,
					"schema":    walEvent.Data.Schema,
				})
			return nil
		}

		natsMsg = natslib.Message{
			Key:   w.getMessageKey(walEvent.Data),
			Value: walDataBytes,
		}
	}

	msg := batch.NewWALMessage(natsMsg, walEvent.CommitPosition)
	return w.batchSender.SendMessage(ctx, msg)
}

func (w *BatchWriter) Name() string {
	return "nats-jetstream-batch-writer"
}

func (w *BatchWriter) Close() error {
	w.batchSender.Close()
	return w.writer.Close()
}

func (w *BatchWriter) sendBatch(ctx context.Context, batch *batch.Batch[natslib.Message]) error {
	messages := batch.GetMessages()
	w.logger.Debug("nats jetstream batch writer: sending message batch", loglib.Fields{
		"batch_size":             len(messages),
		"batch_commit_positions": len(batch.GetCommitPositions()),
	})

	if len(messages) > 0 {
		if err := w.writer.WriteMessages(ctx, messages...); err != nil {
			w.logger.Error(err, "failed to write to nats jetstream")
			return fmt.Errorf("nats jetstream batch writer: writing to nats jetstream: %w", err)
		}
	}

	positions := batch.GetCommitPositions()
	if w.checkpointer != nil && len(positions) > 0 {
		if err := w.checkpointer(ctx, positions); err != nil {
			w.logger.Warn(err, "nats jetstream batch writer: error updating commit position")
		}
	}

	return nil
}

// getMessageKey returns the key to be used in a NATS message for the wal event
// on input. The message key determines which subject the event is routed to,
// and therefore which order the events will be executed in. For DDL events, we
// extract the underlying user schema they're linked to in the content, to make
// sure they're routed to the same subject as their writes. This gives us
// ordering per schema.
func (w BatchWriter) getMessageKey(walData *wal.Data) []byte {
	eventKey := walData.Schema
	if walData.IsDDLEvent() {
		ddlEvent, err := w.walDataToDDLEvent(walData)
		if err != nil {
			w.logger.Error(err, "parsing ddl event for schema", loglib.Fields{
				"wal_data": walData,
			})
			return []byte(eventKey)
		}
		eventKey = ddlEvent.SchemaName
	}

	return []byte(eventKey)
}
