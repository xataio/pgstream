// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/json"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type SnapshotGenerator struct {
	schemalogStore schemalog.Store
	marshaler      func(any) ([]byte, error)
	processor      processor.Processor
	generator      generator.SnapshotGenerator
	logger         loglib.Logger
}

type Option func(*SnapshotGenerator)

const zeroLSN = "0/0"

func NewSnapshotGenerator(schemalogStore schemalog.Store, processor processor.Processor, opts ...Option) *SnapshotGenerator {
	sg := &SnapshotGenerator{
		schemalogStore: schemalogStore,
		processor:      processor,
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

			event, err := s.logEntryToWalEvent(logEntry)
			if err != nil {
				return err
			}

			return s.processor.ProcessWALEvent(ctx, event)
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

func (s *SnapshotGenerator) logEntryToWalEvent(logEntry *schemalog.LogEntry) (*wal.Event, error) {
	schema, err := s.marshaler(logEntry.Schema)
	if err != nil {
		return nil, fmt.Errorf("marshaling log entry schema into json: %w", err)
	}
	return &wal.Event{
		CommitPosition: wal.CommitPosition(zeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Schema:    schemalog.SchemaName,
			Table:     schemalog.TableName,
			LSN:       zeroLSN,
			Columns: []wal.Column{
				{Name: "id", Type: "pgstream.xid", Value: logEntry.ID},
				{Name: "version", Type: "bigint", Value: logEntry.Version},
				{Name: "schema_name", Type: "text", Value: logEntry.SchemaName},
				{Name: "created_at", Type: "timestamp without time zone", Value: logEntry.CreatedAt},
				{Name: "schema", Type: "jsonb", Value: string(schema)},
				{Name: "acked", Type: "boolean", Value: logEntry.Acked},
			},
		},
	}, nil
}
