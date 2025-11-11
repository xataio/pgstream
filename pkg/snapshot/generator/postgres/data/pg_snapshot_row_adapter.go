// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jonboulle/clockwork"
	"github.com/xataio/pgstream/pkg/wal"
)

type adapter struct {
	mapper mapper
	clock  clockwork.Clock
}

func newAdapter(mapper mapper) *adapter {
	return &adapter{
		mapper: mapper,
		clock:  clockwork.NewRealClock(),
	}
}

func (a *adapter) rowToWalEvent(ctx context.Context, tableSchema, tableName string, fieldDescriptions []pgconn.FieldDescription, values []any) *wal.Event {
	if len(fieldDescriptions) == 0 && len(values) == 0 {
		return nil
	}
	return &wal.Event{
		// use 0 since there's no LSN associated, but it can be used as the
		// initial version downstream
		CommitPosition: wal.CommitPosition(wal.ZeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: a.clock.Now().UTC().Format(time.RFC3339),
			LSN:       wal.ZeroLSN,
			Schema:    tableSchema,
			Table:     tableName,
			Columns:   a.toWalEventColumns(ctx, fieldDescriptions, values),
		},
	}
}

func (a *adapter) toWalEventColumns(ctx context.Context, fieldDescriptions []pgconn.FieldDescription, values []any) []wal.Column {
	columns := make([]wal.Column, 0, len(fieldDescriptions))
	for i := range values {
		dataType, err := a.mapper.TypeForOID(ctx, fieldDescriptions[i].DataTypeOID)
		if err != nil {
			continue
		}

		columns = append(columns, wal.Column{
			Name:  fieldDescriptions[i].Name,
			Type:  dataType,
			Value: values[i],
		})
	}

	return columns
}
