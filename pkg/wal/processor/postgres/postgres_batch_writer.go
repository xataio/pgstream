// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a WAL processor implementation that batches and writes wal
// events to a Postgres instance. It coalesces consecutive same-table DML
// events into bulk SQL statements for improved throughput.
type BatchWriter struct {
	*Writer

	batchSender walMessageBatchSender
	dmlAdapter  *dmlAdapter
}

const batchWriter = "postgres_batch_writer"

// NewBatchWriter returns a postgres processor that batches and writes data to
// the configured postgres instance.
func NewBatchWriter(ctx context.Context, config *Config, opts ...WriterOption) (*BatchWriter, error) {
	w, err := newWriter(ctx, config, batchWriter, opts...)
	if err != nil {
		return nil, err
	}

	dml, err := newDMLAdapter(config.OnConflictAction, false, w.logger)
	if err != nil {
		return nil, err
	}

	bw := &BatchWriter{
		Writer:     w,
		dmlAdapter: dml,
	}

	bw.batchSender, err = batch.NewSender(ctx, &config.BatchConfig, bw.sendBatch, w.logger)
	if err != nil {
		return nil, err
	}

	return bw, nil
}

// ProcessWALEvent is called on every new message from the wal. It can be called
// concurrently.
func (w *BatchWriter) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Panic("[PANIC] Panic while processing replication event", loglib.Fields{
				"wal_data":    walEvent,
				"panic":       r,
				"stack_trace": debug.Stack(),
			})

			err = fmt.Errorf("postgres writer: understanding event: %w:  %v", processor.ErrPanic, r)
		}
	}()

	walMsg, err := w.adapter.walEventToMessage(ctx, walEvent)
	if err != nil {
		return err
	}

	msg := batch.NewWALMessage(walMsg, walEvent.CommitPosition)
	if err := w.batchSender.SendMessage(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (w *BatchWriter) Name() string {
	return batchWriter
}

func (w *BatchWriter) Close() error {
	w.logger.Debug("closing batch writer")
	w.batchSender.Close()

	return w.close()
}

func (w *BatchWriter) sendBatch(ctx context.Context, b *batch.Batch[*walMessage]) error {
	messages := b.GetMessages()
	if len(messages) > 0 {
		w.logger.Debug("sending batch", loglib.Fields{"batch_size": len(messages)})

		// Walk messages in order, building "runs" of consecutive
		// same-(schema, table, action) DML events that can be coalesced.
		var currentRun []*walMessage
		var runSchema, runTable, runAction string

		flushRun := func() error {
			if len(currentRun) == 0 {
				return nil
			}
			queries, err := w.buildCoalescedQueries(currentRun)
			if err != nil {
				w.logger.Error(err, "building coalesced queries", loglib.Fields{
					"action": runAction, "schema": runSchema, "table": runTable, "run_size": len(currentRun),
				})
				return err
			}
			if err := w.flushQueries(ctx, queries); err != nil {
				w.logger.Error(err, "flushing coalesced DML queries")
				return err
			}
			currentRun = currentRun[:0]
			return nil
		}

		for _, msg := range messages {
			if msg.IsEmpty() {
				continue
			}

			if msg.isDDL {
				// flush any pending DML run before executing DDL
				if err := flushRun(); err != nil {
					return err
				}

				ddlQueries, err := w.adapter.walEventToQueries(ctx, &wal.Event{Data: msg.data})
				if err != nil {
					w.logger.Error(err, "converting DDL event to queries")
					return err
				}
				for _, q := range ddlQueries {
					if q.IsEmpty() {
						continue
					}
					if _, err := w.pgConn.Exec(ctx, q.sql, q.args...); err != nil {
						w.logger.Error(err, "running DDL query", loglib.Fields{"query_sql": q.sql, "query_args": q.args})
						if !w.isInternalError(err) {
							continue
						}
						return err
					}
				}
				continue
			}

			// check if this message continues the current run
			if len(currentRun) > 0 && (msg.data.Schema != runSchema || msg.data.Table != runTable || msg.data.Action != runAction) {
				if err := flushRun(); err != nil {
					return err
				}
			}

			if len(currentRun) == 0 {
				runSchema = msg.data.Schema
				runTable = msg.data.Table
				runAction = msg.data.Action
			}
			currentRun = append(currentRun, msg)
		}

		// flush any trailing run
		if err := flushRun(); err != nil {
			return err
		}
	}

	if w.checkpointer != nil && len(b.GetCommitPositions()) > 0 {
		return w.checkpointer(ctx, b.GetCommitPositions())
	}

	return nil
}

func (w *BatchWriter) buildCoalescedQueries(run []*walMessage) ([]*query, error) {
	if len(run) == 0 {
		return nil, nil
	}

	action := run[0].data.Action
	switch action {
	case "D":
		events := make([]*wal.Data, len(run))
		for i, m := range run {
			events[i] = m.data
		}
		return w.dmlAdapter.buildBulkDeleteQuery(events)
	case "I":
		events := make([]*wal.Data, len(run))
		for i, m := range run {
			events[i] = m.data
		}
		return w.dmlAdapter.buildBulkInsertQueries(events, run[0].schemaInfo), nil
	default:
		// UPDATE, TRUNCATE, and anything else: individual queries
		queries := make([]*query, 0, len(run))
		for _, m := range run {
			qs, err := w.dmlAdapter.walDataToQueries(m.data, m.schemaInfo)
			if err != nil {
				return nil, err
			}
			queries = append(queries, qs...)
		}
		return queries, nil
	}
}

func (w *BatchWriter) flushQueries(ctx context.Context, queries []*query) error {
	if len(queries) == 0 {
		return nil
	}

	var err error
	for {
		queries, err = w.execQueries(ctx, queries)
		if err != nil {
			return err
		}

		if len(queries) == 0 {
			return nil
		}
	}
}

func (w *BatchWriter) execQueries(ctx context.Context, queries []*query) ([]*query, error) {
	retryQueries := []*query{}
	err := w.pgConn.ExecInTx(ctx, func(tx pglib.Tx) error {
		if err := w.setReplicationRoleToReplica(ctx, tx); err != nil {
			return err
		}

		for i, q := range queries {
			if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
				w.logger.Error(err, "executing sql query", loglib.Fields{
					"sql":      q.sql,
					"args":     q.args,
					"severity": "DATALOSS",
				})
				// if a query returns an error, it will abort the tx. Log it as
				// dataloss and remove it from the list of queries to be
				// retried.
				retryQueries = removeIndex(queries, i)
				return err
			}
		}

		return w.resetReplicationRole(ctx, tx)
	})
	if err != nil && w.isInternalError(err) {
		// if there was an internal error in the tx, there's no point in
		// retrying, return error and stop processing.
		return nil, err
	}

	// if there were no errors or no internal errors in the tx, return the
	// queries to retry (none if the tx was successful)
	return retryQueries, nil
}

func (w *BatchWriter) isInternalError(err error) bool {
	var errRelationDoesNotExist *pglib.ErrRelationDoesNotExist
	var errConstraintViolation *pglib.ErrConstraintViolation
	var errSyntaxError *pglib.ErrSyntaxError
	var errDataException *pglib.ErrDataException
	var errRelationAlreadyExists *pglib.ErrRelationAlreadyExists
	var errPreconditionFailed *pglib.ErrPreconditionFailed
	var errFeatureNotSupported *pglib.ErrFeatureNotSupported
	switch {
	case errors.As(err, &errRelationDoesNotExist),
		errors.As(err, &errConstraintViolation),
		errors.As(err, &errSyntaxError),
		errors.As(err, &errDataException),
		errors.As(err, &errRelationAlreadyExists),
		errors.As(err, &errPreconditionFailed),
		errors.As(err, &errFeatureNotSupported):
		return false
	default:
		return true
	}
}

func removeIndex(s []*query, index int) []*query {
	ret := make([]*query, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
