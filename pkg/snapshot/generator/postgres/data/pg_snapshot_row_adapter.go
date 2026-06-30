// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

type adapter struct {
	mapper mapper
	logger loglib.Logger
}

func newAdapter(mapper mapper, logger loglib.Logger) *adapter {
	return &adapter{
		mapper: mapper,
		logger: logger,
	}
}

// resolvedColumn holds the per-result-set metadata required to build a
// wal.Column. The name and type are resolved once per result set (they are
// identical for every row), leaving only the value to be filled in per row.
type resolvedColumn struct {
	name     string
	dataType string
	srcIndex int
}

// rowEventAdapter converts the rows of a single result set into wal.Events. All
// the per-result-set work (column name/type resolution and timestamp
// formatting) is performed once when the adapter is built, so that the per-row
// path is reduced to copying values into a freshly allocated column slice.
type rowEventAdapter struct {
	schema    string
	table     string
	timestamp string
	columns   []resolvedColumn
	hasFields bool
}

// newRowEventAdapter resolves the column metadata for the given field
// descriptions once, so it can be reused for every row in the result set. The
// field descriptions are identical for all rows, so resolving the type for each
// column OID per row (as well as formatting the timestamp per row) is wasted
// work on large tables.
func (a *adapter) newRowEventAdapter(ctx context.Context, tableSchema, tableName string, fieldDescriptions []pgconn.FieldDescription) *rowEventAdapter {
	columns := make([]resolvedColumn, 0, len(fieldDescriptions))
	for i := range fieldDescriptions {
		dataType, err := a.mapper.TypeForOID(ctx, fieldDescriptions[i].DataTypeOID)
		if err != nil {
			a.logger.Warn(err, "unknown data type OID", loglib.Fields{"data_type_oid": fieldDescriptions[i].DataTypeOID})
			continue
		}

		columns = append(columns, resolvedColumn{
			name:     fieldDescriptions[i].Name,
			dataType: dataType,
			srcIndex: i,
		})
	}

	return &rowEventAdapter{
		schema: tableSchema,
		table:  tableName,
		// use a single timestamp for all the rows in the result set, since
		// formatting it per row is expensive and the snapshot represents a
		// single point in time.
		timestamp: time.Now().UTC().Format(time.RFC3339),
		columns:   columns,
		hasFields: len(fieldDescriptions) > 0,
	}
}

func (r *rowEventAdapter) rowToWalEvent(values []any) *wal.Event {
	if !r.hasFields && len(values) == 0 {
		return nil
	}

	columns := make([]wal.Column, len(r.columns))
	for i, c := range r.columns {
		columns[i] = wal.Column{
			Name:  c.name,
			Type:  c.dataType,
			Value: values[c.srcIndex],
		}
	}

	return &wal.Event{
		// use 0 since there's no LSN associated, but it can be used as the
		// initial version downstream
		CommitPosition: wal.CommitPosition(wal.ZeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: r.timestamp,
			LSN:       wal.ZeroLSN,
			Schema:    r.schema,
			Table:     r.table,
			Columns:   columns,
		},
	}
}
