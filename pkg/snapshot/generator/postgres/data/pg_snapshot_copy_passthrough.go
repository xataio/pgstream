// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	pglibretrier "github.com/xataio/pgstream/internal/postgres/retrier"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"golang.org/x/sync/errgroup"
)

var (
	errMissingCopyPassthroughTarget = errors.New("copy passthrough requires a target postgres url")
	errUnexpectedCopiedRows         = errors.New("number of rows copied doesn't match the source rows")
)

// copyPassthroughSnapshotter streams a page range from the source's COPY TO
// STDOUT into the target's COPY FROM STDIN, decoding nothing on the way. It
// writes to the target itself, so it takes on what the bypassed writer did:
// the target connection and its retry policy, trigger suppression, and a
// budget capping concurrent COPYs.
//
// It delegates the rows COPY cannot carry.
type copyPassthroughSnapshotter struct {
	cfg        *CopyPassthroughConfig
	logger     loglib.Logger
	targetConn pglib.Querier
	budget     synclib.WeightedSemaphore
	fallback   rangeSnapshotter
}

func newCopyPassthroughSnapshotter(ctx context.Context, cfg *CopyPassthroughConfig, logger loglib.Logger,
	instrumentation *otel.Instrumentation, fallback rangeSnapshotter,
) (*copyPassthroughSnapshotter, error) {
	if cfg.TargetURL == "" {
		return nil, errMissingCopyPassthroughTarget
	}

	poolOpts := cfg.poolOptions()
	maxConnections, err := pglib.ConnPoolMaxConnections(cfg.TargetURL, poolOpts...)
	if err != nil {
		return nil, fmt.Errorf("resolving copy passthrough target connections: %w", err)
	}

	var targetConn pglib.Querier
	if cfg.RetryPolicy.DisableRetries {
		targetConn, err = pglib.NewConnPool(ctx, cfg.TargetURL, poolOpts...)
	} else {
		targetConn, err = pglibretrier.NewQuerier(ctx, cfg.RetryPolicy, func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConnPool(ctx, cfg.TargetURL, poolOpts...)
		}, logger)
	}
	if err != nil {
		return nil, fmt.Errorf("connecting to copy passthrough target: %w", err)
	}

	if instrumentation.IsEnabled() {
		targetConn, err = pglibinstrumentation.NewQuerier(targetConn, instrumentation)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("instrumenting copy passthrough target: %w", err), targetConn.Close(ctx))
		}
	}

	if err := targetConn.Ping(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("pinging copy passthrough target: %w", err), targetConn.Close(ctx))
	}

	return &copyPassthroughSnapshotter{
		cfg:        cfg,
		logger:     logger,
		targetConn: targetConn,
		budget:     synclib.NewWeightedSemaphore(synclib.CopyBudgetSize(maxConnections)),
		fallback:   fallback,
	}, nil
}

const targetGeneratedColumnsQuery = `SELECT a.attname::text
FROM pg_catalog.pg_attribute a
  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = $1 AND n.nspname = $2 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''
ORDER BY a.attnum`

// the target rejects these, so the target decides
func (s *copyPassthroughSnapshotter) prepareTable(ctx context.Context, table *table) error {
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

	table.generatedColumns = generated
	return nil
}

func (s *copyPassthroughSnapshotter) close(ctx context.Context) error {
	return errors.Join(s.targetConn.Close(ctx), s.fallback.close(ctx))
}

func (s *copyPassthroughSnapshotter) snapshotRange(ctx context.Context, run runInSnapshotTx, table *table, r pageRange) (int64, error) {
	if !table.hasCopyableColumns() {
		return s.fallback.snapshotRange(ctx, run, table, r)
	}

	// held outside the source tx, not inside it
	if err := s.budget.Acquire(ctx, 1); err != nil {
		return 0, fmt.Errorf("acquiring copy budget: %w", err)
	}
	defer s.budget.Release(1)

	var rowCount int64
	err := run(ctx, func(tx pglib.Tx) error {
		var err error
		rowCount, err = s.copyRange(ctx, tx, table, r)
		return err
	})
	return rowCount, err
}

// the pipe bounds memory
func (s *copyPassthroughSnapshotter) copyRange(ctx context.Context, tx pglib.Tx, table *table, r pageRange) (int64, error) {
	copyTable := table.withoutGeneratedColumns()
	sourceSQL := buildCopyToSQL(copyTable, r)
	targetSQL := buildCopyFromSQL(copyTable)

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
			err = wrapPageRangeQueryError(err)
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
				return fmt.Errorf("copying rows into %s.%s (ctid %d-%d): %w",
					table.schema, table.name, r.start, r.end, err)
			}
			if out := rowsOut.Load(); rowsIn != out {
				return fmt.Errorf("%w: copied (%d), expected (%d)", errUnexpectedCopiedRows, rowsIn, out)
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

	return rowsOut.Load(), nil
}

// no COPY carries all-generated rows
func (t *table) hasCopyableColumns() bool {
	return len(t.withoutGeneratedColumns().columns) > 0
}

// COPY rejects generated columns
func (t *table) withoutGeneratedColumns() *table {
	if len(t.generatedColumns) == 0 {
		return t
	}

	generated := make(map[string]struct{}, len(t.generatedColumns))
	for _, column := range t.generatedColumns {
		generated[column] = struct{}{}
	}

	columns := make([]string, 0, len(t.columns))
	for _, column := range t.columns {
		if _, isGenerated := generated[column]; !isGenerated {
			columns = append(columns, column)
		}
	}

	copied := *t
	copied.columns = columns
	return &copied
}

// reuses the decoding path's query
func buildCopyToSQL(t *table, r pageRange) string {
	return fmt.Sprintf("COPY (%s) TO STDOUT%s", buildPageRangeQuery(t, r), copyFormat)
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
func (s *copyPassthroughSnapshotter) prepareTargetTx(ctx context.Context, tx pglib.Tx) error {
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
