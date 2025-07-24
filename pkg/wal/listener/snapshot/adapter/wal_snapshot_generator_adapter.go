// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"context"
	"time"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
)

// SnapshotGeneratorAdapter adapts a snapshot generator to work with WAL events
type SnapshotGeneratorAdapter struct {
	logger               loglib.Logger
	generator            generator.SnapshotGenerator
	schemaTables         map[string][]string
	schemaExcludedTables map[string][]string
}

type Option func(a *SnapshotGeneratorAdapter)

func NewSnapshotGeneratorAdapter(cfg *SnapshotConfig, generator generator.SnapshotGenerator, opts ...Option) *SnapshotGeneratorAdapter {
	a := &SnapshotGeneratorAdapter{
		generator:            generator,
		schemaTables:         schemaTableMap(cfg.Tables),
		schemaExcludedTables: schemaTableMap(cfg.ExcludedTables),
		logger:               loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func WithLogger(logger loglib.Logger) Option {
	return func(a *SnapshotGeneratorAdapter) {
		a.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "snapshot_generator_adapter",
		})
	}
}

func (s *SnapshotGeneratorAdapter) CreateSnapshot(ctx context.Context) (err error) {
	startTime := time.Now()
	defer func() {
		s.logger.Info("snapshot generation completed", loglib.Fields{"err": err, "duration": time.Since(startTime).String()})
	}()

	snapshot := &snapshot.Snapshot{
		SchemaTables:         s.schemaTables,
		SchemaExcludedTables: s.schemaExcludedTables,
	}
	if err := s.generator.CreateSnapshot(ctx, snapshot); err != nil {
		s.logger.Error(err, "creating snapshot", loglib.Fields{"schemas": snapshot.GetSchemas(), "tables": snapshot.GetTables()})
		return err
	}

	return nil
}

func (s *SnapshotGeneratorAdapter) Close() error {
	s.logger.Info("closing snapshot generator")
	return s.generator.Close()
}
