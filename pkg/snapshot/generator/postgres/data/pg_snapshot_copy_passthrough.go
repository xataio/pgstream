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
	loglib "github.com/xataio/pgstream/pkg/log"
	"golang.org/x/sync/errgroup"
)

var (
	errMissingCopyPassthroughTarget = errors.New("copy passthrough requires a target postgres url")
	errUnexpectedCopiedRows         = errors.New("number of rows copied doesn't match the source rows")
)

// the pipe bounds memory
func (sg *SnapshotGenerator) copyPassthroughRange(ctx context.Context, tx pglib.Tx, table *table, r pageRange) (int64, error) {
	copyTable := table.withoutGeneratedColumns()
	sourceSQL := buildCopyToSQL(copyTable, r)
	targetSQL := buildCopyFromSQL(copyTable)

	sg.logger.Trace("copy passthrough", loglib.Fields{
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
		err := sg.targetConn.ExecInTx(egCtx, func(targetTx pglib.Tx) error {
			if err := sg.prepareTargetTx(egCtx, targetTx); err != nil {
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
func (sg *SnapshotGenerator) prepareTargetTx(ctx context.Context, tx pglib.Tx) error {
	if _, err := tx.Exec(ctx, targetLockTimeout); err != nil {
		return fmt.Errorf("setting lock timeout on postgres target: %w", err)
	}

	if !sg.passthrough.DisableTriggers {
		return nil
	}

	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		return fmt.Errorf("disabling triggers on postgres target: %w", err)
	}
	return nil
}
