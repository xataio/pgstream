// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// runInSnapshotTx runs fn against the source's exported snapshot.
type runInSnapshotTx func(ctx context.Context, fn func(tx pglib.Tx) error) error

// rangeSnapshotter moves the rows of one page range. The generator owns the
// source connection and decides which ranges exist; a snapshotter decides what
// becomes of the rows.
//
// snapshotRange is handed the transaction runner rather than a transaction, so
// a snapshotter that has to wait for something can wait before the transaction
// opens instead of holding one idle while it does.
type rangeSnapshotter interface {
	prepareTable(ctx context.Context, table *table) error
	snapshotRange(ctx context.Context, run runInSnapshotTx, table *table, r pageRange) (int64, error)
	close(ctx context.Context) error
}

// decodingSnapshotter reads the rows, decodes them into Go values and emits
// them to the processor as wal events.
type decodingSnapshotter struct {
	adapter   *adapter
	processor processor.Processor
}

func newDecodingSnapshotter(adapter *adapter, p processor.Processor) *decodingSnapshotter {
	return &decodingSnapshotter{adapter: adapter, processor: p}
}

func (s *decodingSnapshotter) prepareTable(context.Context, *table) error { return nil }

func (s *decodingSnapshotter) close(context.Context) error { return nil }

func (s *decodingSnapshotter) snapshotRange(ctx context.Context, run runInSnapshotTx, table *table, r pageRange) (int64, error) {
	var rowCount int64
	err := run(ctx, func(tx pglib.Tx) error {
		var err error
		rowCount, err = s.readRange(ctx, tx, table, r)
		return err
	})
	return rowCount, err
}

func (s *decodingSnapshotter) readRange(ctx context.Context, tx pglib.Tx, table *table, r pageRange) (int64, error) {
	rows, err := tx.Query(ctx, buildPageRangeQuery(table, r))
	if err != nil {
		return 0, wrapPageRangeQueryError(err)
	}
	defer rows.Close()

	// resolve the column metadata (names/types) and timestamp once per page
	// range, since the field descriptions are identical for every row in the
	// result set.
	rowAdapter := s.adapter.newRowEventAdapter(ctx, table.schema, table.name, rows.FieldDescriptions())
	rowCount := int64(0)
	for rows.Next() {
		rowCount++
		select {
		case <-ctx.Done():
			return rowCount, ctx.Err()
		default:
			values, err := rows.Values()
			if err != nil {
				return rowCount, fmt.Errorf("retrieving rows values: %w", err)
			}

			event := rowAdapter.rowToWalEvent(values)
			if event == nil {
				continue
			}

			if err := s.processor.ProcessWALEvent(ctx, event); err != nil {
				return rowCount, fmt.Errorf("processing snapshot row: %w", err)
			}
		}
	}

	return rowCount, rows.Err()
}
