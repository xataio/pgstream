// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"golang.org/x/sync/errgroup"
)

// SnapshotGeneratorAdapter adapts a snapshot generator to work with WAL events
type SnapshotGeneratorAdapter struct {
	logger          loglib.Logger
	generator       generator.SnapshotGenerator
	schemaTables    map[string][]string
	snapshotWorkers uint
}

type Option func(a *SnapshotGeneratorAdapter)

func NewSnapshotGeneratorAdapter(cfg *SnapshotConfig, generator generator.SnapshotGenerator, opts ...Option) *SnapshotGeneratorAdapter {
	a := &SnapshotGeneratorAdapter{
		generator:       generator,
		schemaTables:    cfg.schemaTableMap(),
		logger:          loglib.NewNoopLogger(),
		snapshotWorkers: cfg.snapshotWorkers(),
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

func (s *SnapshotGeneratorAdapter) CreateSnapshot(ctx context.Context) error {
	errGroup, ctx := errgroup.WithContext(ctx)
	snapshotChan := make(chan *snapshot.Snapshot)
	for i := uint(0); i < s.snapshotWorkers; i++ {
		errGroup.Go(func() error {
			for snapshot := range snapshotChan {
				s.logger.Info("creating snapshot for schema", loglib.Fields{"schema": snapshot.SchemaName, "tables": snapshot.TableNames})
				if err := s.generator.CreateSnapshot(ctx, snapshot); err != nil {
					s.logger.Error(err, "creating snapshot for schema", loglib.Fields{"schema": snapshot.SchemaName, "tables": snapshot.TableNames})
					return err
				}
			}
			return nil
		})
	}
	for schema, tables := range s.schemaTables {
		snapshotChan <- &snapshot.Snapshot{
			SchemaName: schema,
			TableNames: tables,
		}
	}
	close(snapshotChan)

	return errGroup.Wait()
}

func (s *SnapshotGeneratorAdapter) Close() error {
	s.logger.Info("closing snapshot generator")
	return s.generator.Close()
}
