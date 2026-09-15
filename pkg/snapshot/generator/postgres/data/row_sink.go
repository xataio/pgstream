// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

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
			values, err := rowValues(rows)
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

// rowValues returns the values of the current row, with an array of more than
// one dimension decoded in a way that keeps its shape.
//
// rows.Values() flattens such an array. A column that holds {{1,2},{3,4}} comes
// back as [1 2 3 4], pgx writes that as a one dimensional array, and the target
// holds {1,2,3,4}. Nothing reports it: the load reads no error and the counters
// stay at zero.
//
// The dimensions are in the wire data, so a decode into pgtype.Array keeps
// them, and pgx writes them back unchanged. Nested Go slices are not a way out,
// because pgx flattens those as well.
//
// Only an array of more than one dimension is replaced. Everything else keeps
// the type rows.Values() gives it, so a value that already arrives correctly
// does not change shape for a transformer or for another target.
func rowValues(rows pglib.Rows) ([]any, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}

	raw := rows.RawValues()
	fields := rows.FieldDescriptions()
	if len(raw) != len(fields) || len(values) != len(fields) {
		return values, nil
	}

	typeMap := rowTypeMap(rows)
	for i, field := range fields {
		if raw[i] == nil {
			continue
		}
		pgType, found := typeMap.TypeForOID(field.DataTypeOID)
		if !found {
			continue
		}
		if _, isArray := pgType.Codec.(*pgtype.ArrayCodec); !isArray {
			continue
		}
		var array pgtype.Array[any]
		if err := typeMap.Scan(field.DataTypeOID, field.Format, raw[i], &array); err != nil {
			// Keep what rows.Values() gave. A row that loses its shape is
			// better than a load that stops.
			continue
		}
		if len(array.Dims) > 1 {
			values[i] = array
		}
	}
	return values, nil
}

// rowTypeMap returns the type map of the connection the rows came from, so that
// the types pgstream registers are known here too. It falls back to a default
// map, because pgx documents that the connection may be absent.
func rowTypeMap(rows pglib.Rows) *pgtype.Map {
	if conn := rows.Conn(); conn != nil {
		if typeMap := conn.TypeMap(); typeMap != nil {
			return typeMap
		}
	}
	return pgtype.NewMap()
}
