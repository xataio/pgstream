// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// BatchIndexer is the environment for ingesting the WAL logical
// replication events into a search store using the pgstream flow
type BatchIndexer struct {
	store       Store
	adapter     walAdapter
	logger      loglib.Logger
	batchSender batchSender

	skipSchema func(schemaName string) bool

	// checkpoint callback to mark what was safely stored
	checkpoint checkpointer.Checkpoint
}

type Option func(*BatchIndexer)

type batchSender interface {
	AddToBatch(context.Context, *batch.WALMessage[*msg]) error
	Close()
	Send(context.Context) error
}

// NewBatchIndexer returns a processor of wal events that indexes data into the
// search store provided on input.
func NewBatchIndexer(ctx context.Context, config IndexerConfig, store Store, lsnParser replication.LSNParser, opts ...Option) (*BatchIndexer, error) {
	indexer := &BatchIndexer{
		store:  store,
		logger: loglib.NewNoopLogger(),
		// by default all schemas are processed
		skipSchema: func(string) bool { return false },
		adapter:    newAdapter(store.GetMapper(), lsnParser),
	}

	for _, opt := range opts {
		opt(indexer)
	}

	var err error
	indexer.batchSender, err = batch.NewSender(&config.Batch, indexer.sendBatch, indexer.logger)
	if err != nil {
		return nil, err
	}

	// start the send process in the background
	go func() {
		if err := indexer.batchSender.Send(ctx); err != nil {
			indexer.logger.Error(err, "sending stopped")
		}
	}()

	return indexer, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(i *BatchIndexer) {
		i.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "search_batch_indexer",
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(i *BatchIndexer) {
		i.checkpoint = c
	}
}

// ProcessWALEvent is responsible for sending the wal event to the search
// store and committing the event position. It can be called concurrently.
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
		"wal_data":            event.Data,
		"wal_commit_position": event.CommitPosition,
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

	return i.batchSender.AddToBatch(ctx, batch.NewWALMessage(msg, event.CommitPosition))
}

func (i *BatchIndexer) Name() string {
	return "search-batch-indexer"
}

func (i *BatchIndexer) Close() error {
	i.batchSender.Close()
	return nil
}

func (i *BatchIndexer) sendBatch(ctx context.Context, batch *batch.Batch[*msg]) error {
	// we'll mostly process writes, so pre-allocate the "max" amount
	writes := make([]Document, 0, len(batch.GetMessages()))
	flushWrites := func() error {
		if len(writes) > 0 {
			failed, err := i.store.SendDocuments(ctx, writes)
			if err != nil {
				i.logger.Error(err, "search store error when sending documents", loglib.Fields{
					"documents": writes,
				})
				if !errors.Is(err, ErrInvalidQuery) {
					return err
				}
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

	for _, msg := range batch.GetMessages() {
		switch {
		case msg == nil:
			// ignore
		case msg.write != nil:
			writes = append(writes, *msg.write)
		case msg.schemaChange != nil:
			if err := flushWrites(); err != nil {
				return err
			}
			if err := i.applySchemaChange(ctx, msg.schemaChange); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
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
		if err := i.checkpoint(ctx, batch.GetCommitPositions()); err != nil {
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
		if err := i.store.DeleteSchema(ctx, new.SchemaName); err != nil {
			return fmt.Errorf("deleting schema: %w", err)
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
