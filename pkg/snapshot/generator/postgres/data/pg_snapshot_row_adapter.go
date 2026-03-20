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

// tableMetadata holds pre-queried PK column names for a table, used to
// populate Metadata.InternalColIDs on snapshot events so the downstream
// consumer can generate ON CONFLICT for idempotent writes.
type tableMetadata struct {
	tablePgstreamID string
	pkColumnNames   []string
}

func (a *adapter) rowToWalEvent(ctx context.Context, tableSchema, tableName string, fieldDescriptions []pgconn.FieldDescription, values []any, rawValues [][]byte, meta *tableMetadata) *wal.Event {
	if len(fieldDescriptions) == 0 && len(values) == 0 {
		return nil
	}

	columns := a.toWalEventColumns(ctx, fieldDescriptions, values, rawValues)

	// Populate metadata so downstream ON CONFLICT can identify PK columns.
	// Column.ID and Metadata.InternalColIDs use the column name as the ID
	// (matching what the injector would produce for simple PK cases).
	metadata := wal.Metadata{}
	if meta != nil {
		metadata.TablePgstreamID = meta.tablePgstreamID
		metadata.InternalColIDs = meta.pkColumnNames
		// Set Column.ID to column name so extractPrimaryKeyColumns can match
		for i := range columns {
			columns[i].ID = columns[i].Name
		}
	}

	return &wal.Event{
		CommitPosition: wal.CommitPosition(wal.ZeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			LSN:       wal.ZeroLSN,
			Schema:    tableSchema,
			Table:     tableName,
			Columns:   columns,
			Metadata:  metadata,
		},
	}
}

func (a *adapter) toWalEventColumns(ctx context.Context, fieldDescriptions []pgconn.FieldDescription, values []any, rawValues [][]byte) []wal.Column {
	columns := make([]wal.Column, 0, len(fieldDescriptions))
	for i := range values {
		dataType, err := a.mapper.TypeForOID(ctx, fieldDescriptions[i].DataTypeOID)
		if err != nil {
			a.logger.Warn(err, "unknown data type OID", loglib.Fields{"data_type_oid": fieldDescriptions[i].DataTypeOID})
			continue
		}

		// Distinguish SQL NULL from decoded-nil (e.g. JSONB 'null'::jsonb).
		// pgx Values() returns Go nil for both. RawValues() returns nil only
		// for SQL NULL — a decoded-nil like JSONB null has non-nil raw bytes.
		isSQLNull := values[i] == nil && (i >= len(rawValues) || rawValues[i] == nil)

		columns = append(columns, wal.Column{
			Name:   fieldDescriptions[i].Name,
			Type:   dataType,
			Value:  values[i],
			IsSQLNull: isSQLNull,
		})
	}

	return columns
}
