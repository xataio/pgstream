// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/listener"
)

type ProcessEventAdapter struct {
	processor listener.Processor

	clock clockwork.Clock
}

func NewProcessEventAdapter(p listener.Processor) *ProcessEventAdapter {
	return &ProcessEventAdapter{
		processor: p,
		clock:     clockwork.NewRealClock(),
	}
}

func (a *ProcessEventAdapter) ProcessRow(ctx context.Context, row *snapshot.Row) error {
	return a.processor.ProcessWALEvent(ctx, a.snapshotRowToWalEvent(row))
}

func (a *ProcessEventAdapter) Close() error {
	return a.processor.Close()
}

func (a *ProcessEventAdapter) snapshotRowToWalEvent(row *snapshot.Row) *wal.Event {
	if row == nil {
		return nil
	}

	columns := make([]wal.Column, 0, len(row.Columns))
	for _, col := range row.Columns {
		columns = append(columns, a.snapshotColumnToWalColumn(col))
	}
	// use 0 since there's no LSN associated, but it can be used as the
	// initial version downstream
	const zeroLSN = "0/0"
	return &wal.Event{
		CommitPosition: wal.CommitPosition(zeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: a.clock.Now().UTC().Format(time.RFC3339),
			LSN:       zeroLSN,
			Schema:    row.Schema,
			Table:     row.Table,
			Columns:   columns,
		},
	}
}

func (a *ProcessEventAdapter) snapshotColumnToWalColumn(col snapshot.Column) wal.Column {
	return wal.Column{
		Name:  col.Name,
		Type:  col.Type,
		Value: col.Value,
	}
}
