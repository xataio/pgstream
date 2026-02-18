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

func (a *adapter) rowToWalEvent(ctx context.Context, tableSchema, tableName string, fieldDescriptions []pgconn.FieldDescription, values []any, rawValues [][]byte) *wal.Event {
	if len(fieldDescriptions) == 0 && len(values) == 0 {
		return nil
	}
	return &wal.Event{
		// use 0 since there's no LSN associated, but it can be used as the
		// initial version downstream
		CommitPosition: wal.CommitPosition(wal.ZeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			LSN:       wal.ZeroLSN,
			Schema:    tableSchema,
			Table:     tableName,
			Columns:   a.toWalEventColumns(ctx, fieldDescriptions, values, rawValues),
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
