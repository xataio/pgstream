// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
)

type SnapshotGenerator struct {
	schemalogStore schemalog.Store
	marshaler      func(any) ([]byte, error)
	processRow     snapshot.RowProcessor
	generator      generator.SnapshotGenerator
	logger         loglib.Logger
}

type Option func(*SnapshotGenerator)

func NewSnapshotGenerator(schemalogStore schemalog.Store, processRow snapshot.RowProcessor, opts ...Option) *SnapshotGenerator {
	sg := &SnapshotGenerator{
		schemalogStore: schemalogStore,
		processRow:     processRow,
		marshaler:      json.Marshal,
		logger:         loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(sg)
	}

	return sg
}

func WithSnapshotGenerator(g generator.SnapshotGenerator) Option {
	return func(sg *SnapshotGenerator) {
		sg.generator = g
	}
}

func WithLogger(logger loglib.Logger) Option {
	return func(sg *SnapshotGenerator) {
		sg.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_schemalog_snapshot_generator",
		})
	}
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	snapshotErrs := make(snapshot.Errors)
	for schema := range ss.SchemaTables {
		s.logger.Info("creating schema snapshot", loglib.Fields{"schema": schema, "tables": ss.SchemaTables[schema]})
		err := func() error {
			logEntry, err := s.schemalogStore.Insert(ctx, schema)
			if err != nil {
				return err
			}

			s.logger.Debug("created schema log entry", loglib.Fields{
				"schema":     schema,
				"log_entry":  logEntry.ID,
				"version":    logEntry.Version,
				"created_at": logEntry.CreatedAt,
				"acked":      logEntry.Acked,
			})

			row, err := s.logEntryToSnapshotRow(logEntry)
			if err != nil {
				return err
			}

			return s.processRow(ctx, row)
		}()
		if err != nil {
			snapshotErrs.AddError(schema, snapshot.NewSchemaErrors(schema, err))
		}
	}
	if len(snapshotErrs) > 0 {
		return snapshotErrs
	}

	if s.generator != nil {
		return s.generator.CreateSnapshot(ctx, ss)
	}

	return nil
}

func (s *SnapshotGenerator) Close() error {
	if s.generator != nil {
		return s.generator.Close()
	}
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
