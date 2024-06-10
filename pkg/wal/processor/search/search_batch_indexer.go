// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/xataio/pgstream/internal/replication"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// BatchIndexer is the environment for ingesting the WAL logical
// replication events into a search store using the pgstream flow
type BatchIndexer struct {
	store   Store
	adapter walAdapter
	logger  loglib.Logger

	// queueBytesSema is used to limit the amount of memory used by the
	// unbuffered msg channel, optimising the channel performance for variable
	// size messages, while preventing the process from running oom
	queueBytesSema synclib.WeightedSemaphore
	msgChan        chan (*msg)

	batchSize         int
	batchSendInterval time.Duration

	skipSchema func(schemaName string) bool

	// checkpoint callback to mark what was safely stored
	checkpoint checkpointer.Checkpoint

	cleaner cleaner
}

type Option func(*BatchIndexer)

// NewBatchIndexer returns a processor of wal events that indexes data into the
// search store provided on input.
func NewBatchIndexer(ctx context.Context, config IndexerConfig, store Store, lsnParser replication.LSNParser, opts ...Option) *BatchIndexer {
	indexer := &BatchIndexer{
		store:  store,
		logger: loglib.NewNoopLogger(),
		// by default all schemas are processed
		skipSchema:        func(string) bool { return false },
		batchSize:         config.batchSize(),
		batchSendInterval: config.batchTime(),
		adapter:           newAdapter(store.GetMapper(), lsnParser),
		msgChan:           make(chan *msg),
	}

	// this allows us to bound and configure the memory used by the internal msg
	// queue
	indexer.queueBytesSema = synclib.NewWeightedSemaphore(config.maxQueueBytes())

	for _, opt := range opts {
		opt(indexer)
	}

	indexer.cleaner = newSchemaCleaner(&config.CleanupBackoff, store, indexer.logger)
	// start a goroutine for processing schema deletes asynchronously.
	// routine ends when the internal channel is closed.
	go indexer.cleaner.start(ctx)
	return indexer
}

func WithLogger(l loglib.Logger) Option {
	return func(i *BatchIndexer) {
		i.logger = loglib.NewLogger(l)
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(i *BatchIndexer) {
		i.checkpoint = c
	}
}

// ProcessWALEvent is called on every new message from the WAL logical
// replication The function is responsible for sending the data to the search
// store and committing the event position.
func (i *BatchIndexer) ProcessWALEvent(ctx context.Context, event *wal.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			i.logger.Panic("[PANIC] Panic while processing replication event", loglib.Fields{
				"wal_data":    event.Data,
				"panic":       r,
				"stack_trace": debug.Stack(),
			})
			err = fmt.Errorf("search batch indexer: %w: %v", processor.ErrPanic, r)
		}
	}()

	i.logger.Trace("search batch indexer: received wal event", loglib.Fields{
		"wal_data": event.Data,
	})

	msg, err := i.adapter.walEventToMsg(event)
	if err != nil {
		if errors.Is(err, errNilIDValue) || errors.Is(err, errNilVersionValue) || errors.Is(err, errMetadataMissing) {
			i.logger.Warn(err, "search batch indexer: invalid event, skipping message")
			return nil
		}
		return fmt.Errorf("wal data to queue item: %w", err)
	}

	if msg == nil {
		return nil
	}

	// make sure we don't reach the queue memory limit before adding the new
	// message to the channel. This will block until messages have been read
	// from the channel and their size is released
	msgSize := int64(msg.size())
	if !i.queueBytesSema.TryAcquire(msgSize) {
		i.logger.Warn(nil, "search batch indexer: max queue bytes reached, processing blocked")
		if err := i.queueBytesSema.Acquire(ctx, msgSize); err != nil {
			return err
		}
	}
	i.msgChan <- msg

	return nil
}

func (i *BatchIndexer) Send(ctx context.Context) error {
	// make sure we send to the search store on a separate go routine to isolate
	// the IO operations and minimise the wait time between batch sending while
	// continuously building new batches.
	batchChan := make(chan *msgBatch)
	defer close(batchChan)
	sendErrChan := make(chan error, 1)
	go func() {
		defer close(sendErrChan)
		for batch := range batchChan {
			// If the send fails, this goroutine returns an error over the error channel and shuts down.
			err := i.sendBatch(ctx, batch)
			i.queueBytesSema.Release(int64(batch.totalBytes))
			if err != nil {
				i.logger.Error(err, "search batch indexer")
				sendErrChan <- err
				return
			}
		}
	}()

	ticker := time.NewTicker(i.batchSendInterval)
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
		case msg := <-i.msgChan:
			msgBatch.add(msg)
			// trigger a send if we reached the configured batch size or if the
			// event was for a schema change/keep alive. We need to make sure
			// any events following a schema change are processed using the
			// right schema version, and any keep alive messages are
			// checkpointed as soon as possible.
			if msgBatch.size() >= i.batchSize || msg.isSchemaChange() || msg.isKeepAlive() {
				batchChan <- msgBatch.drain()
			}
		}
	}
}

func (i *BatchIndexer) Close() error {
	close(i.msgChan)
	i.cleaner.stop()
	return nil
}

func (i *BatchIndexer) sendBatch(ctx context.Context, batch *msgBatch) error {
	if batch.isEmpty() {
		return nil
	}

	// we'll mostly process writes, so pre-allocate the "max" amount
	writes := make([]Document, 0, len(batch.msgs))
	flushWrites := func() error {
		if len(writes) > 0 {
			failed, err := i.store.SendDocuments(ctx, writes)
			if err != nil {
				return err
			}
			if len(failed) > 0 {
				i.logger.Error(nil, "failed to send documents", loglib.Fields{
					"failed_documents": failed,
				})
			}
			writes = writes[:0]
		}
		return nil
	}

	for _, msg := range batch.msgs {
		switch {
		case msg.write != nil:
			writes = append(writes, *msg.write)
		case msg.schemaChange != nil:
			if err := flushWrites(); err != nil {
				return err
			}
			if err := i.applySchemaChange(ctx, msg.schemaChange); err != nil {
				i.logDataLoss(msg.schemaChange, err)
				return nil
			}
		case msg.truncate != nil:
			if err := flushWrites(); err != nil {
				return err
			}
			if err := i.truncateTable(ctx, msg.truncate); err != nil {
				return err
			}
		default:
			return errEmptyQueueMsg
		}
	}

	if err := flushWrites(); err != nil {
		return err
	}

	if i.checkpoint != nil {
		if err := i.checkpoint(ctx, batch.positions); err != nil {
			return fmt.Errorf("checkpointing positions: %w", err)
		}
	}

	return nil
}

func (i *BatchIndexer) truncateTable(ctx context.Context, item *truncateItem) error {
	return i.store.DeleteTableDocuments(ctx, item.schemaName, []string{item.tableID})
}

func (i *BatchIndexer) applySchemaChange(ctx context.Context, new *schemalog.LogEntry) error {
	// schema is filtered out, nothing to do
	if i.skipSchema(new.SchemaName) {
		i.logger.Info("search batch indexer: apply schema change: skipping schema", loglib.Fields{"schema_name": new.SchemaName})
		return nil
	}

	if new.Schema.Dropped {
		if err := i.cleaner.deleteSchema(ctx, new.SchemaName); err != nil {
			return fmt.Errorf("register schema for delete: %w", err)
		}
		return nil
	}

	i.logger.Info("search batch indexer: apply schema change", loglib.Fields{
		"logEntry": map[string]any{
			"ID":        new.ID.String(),
			"version":   new.Version,
			"schema":    new.SchemaName,
			"isDropped": new.Schema.Dropped,
		},
	})

	if err := i.store.ApplySchemaChange(ctx, new); err != nil {
		return fmt.Errorf("applying schema change: %w", err)
	}

	return nil
}

func (i *BatchIndexer) logDataLoss(logEntry *schemalog.LogEntry, err error) {
	i.logger.Error(err, "search batch indexer", loglib.Fields{
		"severity": "DATALOSS",
		"logEntry": map[string]any{
			"ID":      logEntry.ID.String(),
			"version": logEntry.Version,
			"schema":  logEntry.SchemaName,
		},
	})
}
