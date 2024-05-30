// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/xataio/pgstream/internal/backoff"
	synclib "github.com/xataio/pgstream/internal/sync"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// BatchIndexer is the environment for ingesting the WAL logical
// replication events into a search store using the pgstream flow
type BatchIndexer struct {
	store   Store
	adapter walAdapter

	// queueBytesSema is used to limit the amount of memory used by the
	// unbuffered msg channel, optimising the channel performance for variable
	// size messages, while preventing the process from running oom
	queueBytesSema synclib.WeightedSemaphore
	msgChan        chan (*msg)

	batchSize         int
	batchSendInterval time.Duration

	skipSchema func(schemaName string) bool

	// checkpoint callback to mark what was safely stored
	checkpoint checkpoint

	cleaner cleaner
}

type IndexerConfig struct {
	BatchSize      int
	BatchTime      time.Duration
	CleanupBackoff backoff.Config
	MaxQueueBytes  int
}

// checkpoint defines the way to confirm the positions that have been read.
// The actual implementation depends on the source of events (postgres, kafka,...)
type checkpoint func(ctx context.Context, positions []wal.CommitPosition) error

const defaultMaxQueueBytes = 100 * 1024 * 1024 // 100MiB

// NewBatchIndexer returns a processor of wal events that indexes data into the
// search store provided on input.
func NewBatchIndexer(ctx context.Context, config IndexerConfig, store Store) *BatchIndexer {
	indexer := &BatchIndexer{
		store: store,
		// by default all schemas are processed
		skipSchema:        func(string) bool { return false },
		batchSize:         config.BatchSize,
		batchSendInterval: config.BatchTime,
		cleaner:           newSchemaCleaner(&config.CleanupBackoff, store),
		adapter:           newAdapter(store.GetMapper()),
		msgChan:           make(chan *msg),
	}

	// this allows us to bound and configure the memory used by the internal msg
	// queue
	maxQueueBytes := defaultMaxQueueBytes
	if config.MaxQueueBytes > 0 {
		maxQueueBytes = config.MaxQueueBytes
	}
	indexer.queueBytesSema = synclib.NewWeightedSemaphore(int64(maxQueueBytes))

	// start a goroutine for processing schema deletes asynchronously.
	// routine ends when the internal channel is closed.
	go indexer.cleaner.start(ctx)
	return indexer
}

// ProcessWALEvent is called on every new message from the WAL logical
// replication The function is responsible for sending the data to the search
// store and committing the event position.
func (i *BatchIndexer) ProcessWALEvent(ctx context.Context, event *wal.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.WithLevel(zerolog.PanicLevel).
				Any("wal_data", event.Data).
				Any("panic", r).
				Bytes("stack_trace", debug.Stack()).
				Msg("[PANIC] Panic while processing replication event")

			err = fmt.Errorf("search batch indexer: %w: %v", processor.ErrPanic, r)
		}
	}()

	msg, err := i.adapter.walEventToMsg(event)
	if err != nil {
		if errors.Is(err, errNilIDValue) || errors.Is(err, errNilVersionValue) || errors.Is(err, errMetadataMissing) {
			log.Warn().Msgf("invalid event, skipping message: %v", err)
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
		log.Warn().Msg("search batch indexer: max queue bytes reached, processing blocked")
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
				log.Error().Err(err).Msg("search batch indexer")
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
			batchChan <- msgBatch.drain()
		case msg := <-i.msgChan:
			// trigger a send if we reached the configured batch size or if the
			// event was for a schema change. We need to make sure any events
			// following a schema change are processed using the right schema
			// version
			if msgBatch.size() >= i.batchSize || msg.isSchemaChange() {
				batchChan <- msgBatch.drain()
			}
			msgBatch.add(msg)
		}
	}
}

func (i *BatchIndexer) SetCheckpoint(checkpoint checkpoint) {
	i.checkpoint = checkpoint
}

func (i *BatchIndexer) Close() error {
	close(i.msgChan)
	i.cleaner.stop()
	return nil
}

func (i *BatchIndexer) sendBatch(ctx context.Context, batch *msgBatch) error {
	if len(batch.msgs) == 0 {
		return nil
	}

	// we'll mostly process writes, so pre-allocate the "max" amount
	writes := make([]Document, 0, len(batch.msgs))
	flushWrites := func() error {
		if len(writes) > 0 {
			if _, err := i.store.SendDocuments(ctx, writes); err != nil {
				return err
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
				logDataLoss(msg.schemaChange, err)
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
		log.Info().Msgf("applySchemaChange: skipping schema [%s]", new.SchemaName)
		return nil
	}

	if new.Schema.Dropped {
		if err := i.cleaner.deleteSchema(ctx, new.SchemaName); err != nil {
			return fmt.Errorf("register schema for delete: %w", err)
		}
		return nil
	}

	log.Info().Dict("logEntry", zerolog.Dict().
		Str("ID", new.ID.String()).
		Int64("version", new.Version).
		Str("schema", new.SchemaName).
		Bool("isDropped", new.Schema.Dropped)).Msg("search batch indexer: apply schema change")

	if err := i.store.ApplySchemaChange(ctx, new); err != nil {
		return fmt.Errorf("applying schema change: %w", err)
	}

	return nil
}

func logDataLoss(logEntry *schemalog.LogEntry, err error) {
	log.Error().Err(err).
		Str("severity", "DATALOSS").
		Dict("logEntry", zerolog.Dict().
			Str("ID", logEntry.ID.String()).
			Int64("version", logEntry.Version).
			Str("schema", logEntry.SchemaName)).
		Msg("search batch indexer")
}
