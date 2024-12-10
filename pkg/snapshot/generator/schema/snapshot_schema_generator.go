// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/snapshot"
)

type SnapshotGenerator struct {
	schemalogStore schemalog.Store
	marshaler      func(any) ([]byte, error)
	processRow     snapshot.RowProcessor
}

func NewSnapshotGenerator(schemalogStore schemalog.Store, processRow snapshot.RowProcessor) *SnapshotGenerator {
	return &SnapshotGenerator{
		schemalogStore: schemalogStore,
		processRow:     processRow,
		marshaler:      json.Marshal,
	}
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	err := func() error {
		logEntry, err := s.schemalogStore.Insert(ctx, ss.SchemaName)
		if err != nil {
			return err
		}

		row, err := s.logEntryToSnapshotRow(logEntry)
		if err != nil {
			return err
		}

		return s.processRow(ctx, row)
	}()
	if err != nil {
		return &snapshot.Errors{Snapshot: err}
	}

	return nil
}

func (s *SnapshotGenerator) Close() error {
	return nil
}

func (s *SnapshotGenerator) logEntryToSnapshotRow(logEntry *schemalog.LogEntry) (*snapshot.Row, error) {
	schema, err := s.marshaler(logEntry.Schema)
	if err != nil {
		return nil, fmt.Errorf("marshaling log entry schema into json: %w", err)
	}
	return &snapshot.Row{
		Schema: schemalog.SchemaName,
		Table:  schemalog.TableName,
		Columns: []snapshot.Column{
			{Name: "id", Type: "pgstream.xid", Value: logEntry.ID},
			{Name: "version", Type: "bigint", Value: logEntry.Version},
			{Name: "schema_name", Type: "text", Value: logEntry.SchemaName},
			{Name: "created_at", Type: "timestamp without time zone", Value: logEntry.CreatedAt},
			{Name: "schema", Type: "jsonb", Value: string(schema)},
			{Name: "acked", Type: "boolean", Value: logEntry.Acked},
		},
	}, nil
}
