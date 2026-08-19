// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// rowSink turns the rows a reading strategy queries into the wal events the
// snapshot emits. It owns everything that happens to a row once it has been
// read: adapting it to a wal event, handing it to the processor and reporting
// the bytes read. A strategy is therefore only responsible for deciding which
// rows to read, and every strategy consumes them the same way.
type rowSink struct {
	adapter   *adapter
	processor processor.Processor
	progress  progressTracker
}

func newRowSink(mapper mapper, processor processor.Processor, logger loglib.Logger, progress progressTracker) rowSink {
	return rowSink{
		adapter:   newAdapter(mapper, logger),
		processor: processor,
		progress:  progress,
	}
}

// emit converts every row of the result set into a wal event and hands it to
// the processor, reporting the bytes read against the table's schema once the
// result set has been fully consumed. It returns the number of rows read.
func (s rowSink) emit(ctx context.Context, table *table, rows pglib.Rows) (uint, error) {
	// resolve the column metadata (names/types) and timestamp once per result
	// set, since the field descriptions are identical for every row in it.
	rowAdapter := s.adapter.newRowEventAdapter(ctx, table.schema, table.name, rows.FieldDescriptions())
	rowCount := uint(0)
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

	s.progress.advance(table.schema, int64(rowCount)*table.rowSize)

	return rowCount, rows.Err()
}
