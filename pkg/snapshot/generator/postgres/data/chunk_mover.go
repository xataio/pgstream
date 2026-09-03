// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
)

// runInTx runs fn against a connection that can read the chunk. A reading
// strategy supplies its own: the ctid reader imports the exported transaction
// snapshot, a keyset reader need not open one at all.
type runInTx func(ctx context.Context, fn func(tx pglib.Tx) error) error

// chunkMover moves the rows a reading strategy has selected. The strategy
// decides which rows to read and says so as a complete query; the mover decides
// what becomes of them, so every strategy gets every mover.
//
// It takes the query rather than the rows because the copy passthrough must
// never run it as an ordinary query: it wraps it in COPY. It takes a way to run
// rather than an open transaction because the passthrough retries a whole chunk
// on a fresh pipe, which needs a fresh transaction.
//
// The query must be complete SQL. COPY accepts no bind parameters, so a
// strategy that phrases its chunk with placeholders can only ever be decoded.
type chunkMover interface {
	// prepareTable resolves whatever the mover needs before the table's chunks
	// are moved, once its columns are known.
	prepareTable(ctx context.Context, table *table) error
	// move reads the chunk the query selects and returns the rows moved.
	move(ctx context.Context, run runInTx, table *table, query string) (uint, error)
	close(ctx context.Context) error
}

// newChunkMover picks how a chunk is moved. Selecting it here, next to the
// sink and before any reader exists, is what keeps it independent of the
// reading strategy.
func newChunkMover(ctx context.Context, cfg *Config, logger loglib.Logger,
	instrumentation *otel.Instrumentation, progress progressTracker, decoding chunkMover,
) (chunkMover, error) {
	if cfg.CopyPassthrough == nil {
		return decoding, nil
	}
	return newCopyPassthroughMover(ctx, cfg.CopyPassthrough, logger, instrumentation, progress, decoding)
}

// decodingMover reads the rows and hands them to the sink, which adapts them
// into wal events for the processor.
type decodingMover struct {
	sink rowSink
}

func newDecodingMover(sink rowSink) decodingMover {
	return decodingMover{sink: sink}
}

func (m decodingMover) prepareTable(context.Context, *table) error { return nil }

func (m decodingMover) close(context.Context) error { return nil }

func (m decodingMover) move(ctx context.Context, run runInTx, table *table, query string) (uint, error) {
	var rowCount uint
	err := run(ctx, func(tx pglib.Tx) error {
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return wrapChunkQueryError(err)
		}
		defer rows.Close()

		rowCount, err = m.sink.emit(ctx, table, rows)
		return err
	})
	return rowCount, err
}

// a vanished relation means schema drift
func wrapChunkQueryError(err error) error {
	var relationErr *pglib.ErrRelationDoesNotExist
	if errors.As(err, &relationErr) {
		return fmt.Errorf("%w: querying table rows: %w", ErrSchemaChangedDuringSnapshot, err)
	}
	return fmt.Errorf("querying table rows: %w", err)
}
