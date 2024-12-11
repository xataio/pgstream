// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/data/postgres"
	"github.com/xataio/pgstream/pkg/snapshot/generator/schema"
	pgsnapshotstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"golang.org/x/sync/errgroup"
)

// SnapshotGeneratorAdapter adapts a snapshot generator to work with WAL events
type SnapshotGeneratorAdapter struct {
	logger          loglib.Logger
	generator       generator.SnapshotGenerator
	processEvent    listenerProcessWalEvent
	schemaTables    map[string][]string
	snapshotWorkers uint
	clock           clockwork.Clock
}

const (
	publicSchema = "public"
)

func NewSnapshotGeneratorAdapter(ctx context.Context, cfg *SnapshotConfig, processEvent listenerProcessWalEvent, opts ...pgsnapshotgenerator.Option) (*SnapshotGeneratorAdapter, error) {
	s := &SnapshotGeneratorAdapter{
		processEvent:    processEvent,
		schemaTables:    cfg.schemaTableMap(),
		logger:          loglib.NewNoopLogger(),
		snapshotWorkers: cfg.snapshotWorkers(),
		clock:           clockwork.NewRealClock(),
	}

	// postgres schema snapshot generator
	var schemaLogStore schemalog.Store
	var err error
	schemaLogStore, err = schemalogpg.NewStore(ctx, cfg.SchemaLogStore)
	if err != nil {
		return nil, fmt.Errorf("create schema log postgres store: %w", err)
	}
	schemaLogStore = schemalog.NewStoreCache(schemaLogStore)
	schemaSnapshotGenerator := schema.NewSnapshotGenerator(schemaLogStore, s.processRow)

	// postgres data snapshot generator
	dataSnapshotGenerator, err := pgsnapshotgenerator.NewSnapshotGenerator(ctx, &cfg.Generator, s.processRow, opts...)
	if err != nil {
		return nil, err
	}

	s.generator = generator.NewAggregator([]generator.SnapshotGenerator{schemaSnapshotGenerator, dataSnapshotGenerator})
	// snapshot activity recorder layer
	snapshotStore, err := pgsnapshotstore.New(ctx, cfg.SnapshotStoreURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres snapshot store: %w", err)
	}
	s.generator = generator.NewSnapshotRecorder(snapshotStore, s.generator)

	return s, nil
}

func (s *SnapshotGeneratorAdapter) CreateSnapshot(ctx context.Context) error {
	errGroup, ctx := errgroup.WithContext(ctx)
	snapshotChan := make(chan *snapshot.Snapshot)
	for i := uint(0); i < s.snapshotWorkers; i++ {
		errGroup.Go(func() error {
			for snapshot := range snapshotChan {
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

func (s *SnapshotGeneratorAdapter) processRow(ctx context.Context, row *snapshot.Row) error {
	return s.processEvent(ctx, s.snapshotRowToWalEvent(row))
}

func (s *SnapshotGeneratorAdapter) snapshotRowToWalEvent(row *snapshot.Row) *wal.Event {
	if row == nil {
		return nil
	}

	columns := make([]wal.Column, 0, len(row.Columns))
	for _, col := range row.Columns {
		columns = append(columns, s.snapshotColumnToWalColumn(col))
	}
	// use 0 since there's no LSN associated, but it can be used as the
	// initial version downstream
	const zeroLSN = "0/0"
	return &wal.Event{
		CommitPosition: wal.CommitPosition(zeroLSN),
		Data: &wal.Data{
			Action:    "I",
			Timestamp: s.clock.Now().UTC().Format(time.RFC3339),
			LSN:       zeroLSN,
			Schema:    row.Schema,
			Table:     row.Table,
			Columns:   columns,
		},
	}
}

func (s *SnapshotGeneratorAdapter) snapshotColumnToWalColumn(col snapshot.Column) wal.Column {
	return wal.Column{
		Name:  col.Name,
		Type:  col.Type,
		Value: col.Value,
	}
}
