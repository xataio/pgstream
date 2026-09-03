// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	pglibretrier "github.com/xataio/pgstream/internal/postgres/retrier"
	synclib "github.com/xataio/pgstream/internal/sync"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"golang.org/x/sync/errgroup"
)

var (
	errMissingCopyPassthroughTarget = errors.New("copy passthrough requires a target postgres url")
	errUnexpectedCopiedRows         = errors.New("number of rows copied doesn't match the source rows")
	errChunkAlreadyCommitted        = errors.New("the target committed the page range before the source transaction failed")
)

// copyPassthroughMover streams a page range from the source's COPY TO
// STDOUT into the target's COPY FROM STDIN, decoding nothing on the way. It
// writes to the target itself, so it takes on what the bypassed writer did:
// the target connection and its retry policy, trigger suppression, and a
// budget capping concurrent COPYs.
//
// It delegates the rows COPY cannot carry.
type copyPassthroughMover struct {
	cfg             *CopyPassthroughConfig
	logger          loglib.Logger
	progress        progressTracker
	targetConn      pglib.Querier
	budget          synclib.WeightedSemaphore
	fallback        chunkMover
	backoffProvider backoff.Provider
}

func newCopyPassthroughMover(ctx context.Context, cfg *CopyPassthroughConfig, logger loglib.Logger,
	instrumentation *otel.Instrumentation, progress progressTracker, fallback chunkMover,
) (*copyPassthroughMover, error) {
	if cfg.TargetURL == "" {
		return nil, errMissingCopyPassthroughTarget
	}

	poolOpts := cfg.poolOptions()
	maxConnections, err := pglib.ConnPoolMaxConnections(cfg.TargetURL, poolOpts...)
	if err != nil {
		return nil, fmt.Errorf("resolving copy passthrough target connections: %w", err)
	}

	// not the retrying querier: it replays the transaction closure, and the
	// closure reads a pipe the failed attempt already drained. Retrying is
	// done a page range at a time instead, where the pipe is rebuilt.
	pool, err := pglib.NewConnPool(ctx, cfg.TargetURL, poolOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to copy passthrough target: %w", err)
	}
	targetConn := pglib.Querier(pool)

	if instrumentation.IsEnabled() {
		// release the pool, not the instrumented querier: NewQuerier returns a
		// nil Querier when it fails, and the pool is what has to be closed
		instrumented, err := pglibinstrumentation.NewQuerier(targetConn, instrumentation)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("instrumenting copy passthrough target: %w", err), pool.Close(ctx))
		}
		targetConn = instrumented
	}

	if err := targetConn.Ping(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("pinging copy passthrough target: %w", err), targetConn.Close(ctx))
	}

	return &copyPassthroughMover{
		cfg:             cfg,
		logger:          logger,
		progress:        progress,
		targetConn:      targetConn,
		budget:          synclib.NewWeightedSemaphore(synclib.CopyBudgetSize(maxConnections)),
		fallback:        fallback,
		backoffProvider: backoff.NewProvider(&cfg.RetryPolicy),
	}, nil
}

const targetGeneratedColumnsQuery = `SELECT a.attname::text
FROM pg_catalog.pg_attribute a
  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = $1 AND n.nspname = $2 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''
ORDER BY a.attnum`

// the target rejects these, so the target decides
func (s *copyPassthroughMover) prepareTable(ctx context.Context, table *table) error {
	if err := s.fallback.prepareTable(ctx, table); err != nil {
		return err
	}

	rows, err := s.targetConn.Query(ctx, targetGeneratedColumnsQuery,
		pglib.UnquoteIdentifier(table.name), pglib.UnquoteIdentifier(table.schema))
	if err != nil {
		return fmt.Errorf("getting target generated columns for %s.%s: %w", table.schema, table.name, err)
	}
	defer rows.Close()

	var generated []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return fmt.Errorf("scanning target generated column: %w", err)
		}
		generated = append(generated, column)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	copyable := withoutColumns(table.columns, generated)
	if len(generated) > 0 && len(copyable) == 0 {
		// every column is generated, so no COPY can carry the table's rows and
		// the decoding path moves them. The reader's own column list is left
		// alone: emptying it widens its query to every column, the generated
		// ones included, which the target then rejects on insert.
		table.decodeOnly = true
		return nil
	}

	// prune what the strategy will select, so its query and the target COPY
	// name the same columns: the mover no longer builds both statements
	table.columns = copyable
	return nil
}

func withoutColumns(columns, remove []string) []string {
	if len(remove) == 0 {
		return columns
	}

	removed := make(map[string]struct{}, len(remove))
	for _, column := range remove {
		removed[column] = struct{}{}
	}

	kept := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, drop := removed[column]; !drop {
			kept = append(kept, column)
		}
	}
	return kept
}

func (s *copyPassthroughMover) close(ctx context.Context) error {
	return errors.Join(s.targetConn.Close(ctx), s.fallback.close(ctx))
}

func (s *copyPassthroughMover) move(ctx context.Context, run runInTx, table *table, query string) (uint, error) {
	// prepareTable found rows COPY cannot carry: the decoding path moves them,
	// and reports its own progress through the sink
	if table.decodeOnly {
		return s.fallback.move(ctx, run, table, query)
	}

	// held outside the source tx, not inside it
	if err := s.budget.Acquire(ctx, 1); err != nil {
		return 0, fmt.Errorf("acquiring copy budget: %w", err)
	}
	defer s.budget.Release(1)

	rowCount, err := s.copyChunkWithRetry(ctx, run, table, query)
	if err != nil {
		return rowCount, err
	}

	// the sink reports the bytes the decoding path moves; nothing reports this
	// path's, so the mover does it here
	s.progress.advance(table.schema, int64(rowCount)*table.rowSize)
	return rowCount, nil
}

func (s *copyPassthroughMover) copyChunkWithRetry(ctx context.Context, run runInTx, table *table, query string) (uint, error) {
	rowCount, err := s.copyChunk(ctx, run, table, query)
	if err == nil || s.cfg.RetryPolicy.DisableRetries || !retriableCopyError(err) {
		return rowCount, err
	}

	// a page range is re-runnable while the target transaction rolled back, and
	// the source is read from the same exported snapshot. copyChunk marks the
	// failures that land after the target commit permanent, so they never reach
	// this loop.
	err = s.backoffProvider(ctx).RetryNotify(func() error {
		var retryErr error
		rowCount, retryErr = s.copyChunk(ctx, run, table, query)
		if retryErr != nil && !retriableCopyError(retryErr) {
			return fmt.Errorf("%w: %w", retryErr, backoff.ErrPermanent)
		}
		return retryErr
	}, func(err error, d time.Duration) {
		s.logger.Warn(err, "retrying copy passthrough chunk", loglib.Fields{
			"schema": table.schema, "table": table.name, "retry_delay": d.String(),
		})
	})
	return rowCount, err
}

// an integrity assertion is not a transient failure
func retriableCopyError(err error) bool {
	return !errors.Is(err, backoff.ErrPermanent) && pglibretrier.IsRetriableError(err)
}

func (s *copyPassthroughMover) copyChunk(ctx context.Context, run runInTx, table *table, query string) (uint, error) {
	var (
		rowCount  uint
		committed bool
	)
	err := run(ctx, func(tx pglib.Tx) error {
		var err error
		rowCount, err = s.copyChunkInTx(ctx, tx, table, query)
		// the target transaction is nested in this one and commits before it,
		// so a nil here means the rows are already durable on the target
		committed = err == nil
		return err
	})
	if err != nil && committed {
		// the source transaction failed after the target had committed the
		// range. Copying it again would write the same rows twice, which no
		// row count assertion can see, so fail the table instead.
		return rowCount, fmt.Errorf("%w: %w: %w", errChunkAlreadyCommitted, err, backoff.ErrPermanent)
	}
	return rowCount, err
}

// the pipe bounds memory
func (s *copyPassthroughMover) copyChunkInTx(ctx context.Context, tx pglib.Tx, table *table, query string) (uint, error) {
	sourceSQL := buildCopyToSQL(query)
	targetSQL := buildCopyFromSQL(table)

	s.logger.Trace("copy passthrough", loglib.Fields{
		"schema": table.schema, "table": table.name,
		"source_sql": sourceSQL, "target_sql": targetSQL,
	})

	pr, pw := io.Pipe()
	eg, egCtx := errgroup.WithContext(ctx)

	var rowsOut atomic.Int64
	eg.Go(func() error {
		n, err := tx.CopyToWriter(egCtx, pw, sourceSQL)
		if err != nil {
			err = wrapChunkQueryError(err)
		} else {
			rowsOut.Store(n)
		}
		// unblocks a target awaiting rows
		pw.CloseWithError(err)
		return err
	})

	eg.Go(func() error {
		err := s.targetConn.ExecInTx(egCtx, func(targetTx pglib.Tx) error {
			if err := s.prepareTargetTx(egCtx, targetTx); err != nil {
				return err
			}
			rowsIn, err := targetTx.CopyFromReader(egCtx, pr, targetSQL)
			if err != nil {
				return fmt.Errorf("copying rows into %s.%s: %w", table.schema, table.name, err)
			}
			if out := rowsOut.Load(); rowsIn != out {
				return fmt.Errorf("%w: copied (%d), expected (%d): %w",
					errUnexpectedCopiedRows, rowsIn, out, backoff.ErrPermanent)
			}
			return nil
		})
		// unblocks a source mid-write
		pr.CloseWithError(err)
		return err
	})

	if err := eg.Wait(); err != nil {
		return 0, err
	}

	return uint(rowsOut.Load()), nil
}

// the strategy's own query, which must carry no bind parameters
func buildCopyToSQL(query string) string {
	return fmt.Sprintf("COPY (%s) TO STDOUT%s", query, copyFormat)
}

// survives target column reordering
func buildCopyFromSQL(t *table) string {
	target := pglib.QuoteQualifiedIdentifier(t.schema, t.name)
	if len(t.columns) == 0 {
		return fmt.Sprintf("COPY %s FROM STDIN%s", target, copyFormat)
	}

	quotedColumns := make([]string, len(t.columns))
	for i, column := range t.columns {
		quotedColumns[i] = pglib.QuoteRawIdentifier(column)
	}
	return fmt.Sprintf("COPY %s (%s) FROM STDIN%s", target, strings.Join(quotedColumns, ", "), copyFormat)
}

// bounds the wait, does not cap the copy
const targetLockTimeout = "SET LOCAL lock_timeout = '30s'"

// the bypassed writer did this
func (s *copyPassthroughMover) prepareTargetTx(ctx context.Context, tx pglib.Tx) error {
	if _, err := tx.Exec(ctx, targetLockTimeout); err != nil {
		return fmt.Errorf("setting lock timeout on postgres target: %w", err)
	}

	if !s.cfg.DisableTriggers {
		return nil
	}

	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		return fmt.Errorf("disabling triggers on postgres target: %w", err)
	}
	return nil
}
