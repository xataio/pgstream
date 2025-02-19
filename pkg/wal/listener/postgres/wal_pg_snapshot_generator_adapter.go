// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestoregenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	schemalogsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/schemalog"
	pgtablefinder "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/tablefinder"
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

var errSchemaSnapshotNotConfigured = errors.New("no schema snapshot has been configured")

// NewSnapshotGeneratorAdapter builds a snapshot generator based on the
// configuration on input, adapting the process wal event to fit with the
// snapshot package implementation. It will add an activity recorder layer if
// configured which will keep track of snapshot requests and their status.
//
// ┌───────────────────────────────────────────────────┐
// │               Snapshot Generator Adapter          │
// │ ┌─────────┐ ┌────────┐ ┌────────────────────────┐ │
// │ │         │ │        │ │       Aggregator       │ │
// │ │         │ │        │ │  ┌──────────────────┐  │ │
// │ │         │ │        │ │  │ Schema Snapshot  │  │ │
// │ │Snapshot │ │ Table  │ │  └─────────┬────────┘  │ │
// │ │Activity │─▶ Finder ├▶│            │           │ │
// │ │Recorder │ │        │ │  ┌─────────▼────────┐  │ │
// │ │         │ │        │ │  │  Data Snapshot   │  │ │
// │ │         │ │        │ │  └──────────────────┘  │ │
// │ └─────────┘ └────────┘ └────────────────────────┘ │
// └───────────────────────────────────────────────────┘
func NewSnapshotGeneratorAdapter(ctx context.Context, cfg *SnapshotConfig, processEvent listenerProcessWalEvent, logger loglib.Logger) (*SnapshotGeneratorAdapter, error) {
	s := &SnapshotGeneratorAdapter{
		processEvent:    processEvent,
		schemaTables:    cfg.schemaTableMap(),
		logger:          loglib.NewNoopLogger(),
		snapshotWorkers: cfg.snapshotWorkers(),
		clock:           clockwork.NewRealClock(),
	}

	// postgres schema snapshot generator
	schemaSnapshotGenerator, err := s.newSchemaSnapshotGenerator(ctx, cfg.Schema, logger)
	if err != nil {
		return nil, err
	}

	// postgres data snapshot generator
	dataSnapshotGenerator, err := pgsnapshotgenerator.NewSnapshotGenerator(ctx, &cfg.Generator, s.processRow, pgsnapshotgenerator.WithLogger(logger))
	if err != nil {
		return nil, err
	}

	// snapshot generator aggregator
	s.generator = generator.NewAggregator([]generator.SnapshotGenerator{schemaSnapshotGenerator, dataSnapshotGenerator})

	// snapshot table finder layer
	s.generator, err = pgtablefinder.NewSnapshotTableFinder(ctx, cfg.Generator.URL, s.generator)
	if err != nil {
		return nil, err
	}

	if cfg.SnapshotStoreURL != "" {
		// snapshot activity recorder layer
		snapshotStore, err := pgsnapshotstore.New(ctx, cfg.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("create postgres snapshot store: %w", err)
		}
		s.generator = generator.NewSnapshotRecorder(snapshotStore, s.generator)
	}

	return s, nil
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

func (s *SnapshotGeneratorAdapter) newSchemaSnapshotGenerator(ctx context.Context, cfg SchemaSnapshotConfig, logger loglib.Logger) (generator.SnapshotGenerator, error) {
	switch {
	case cfg.SchemaLogStore != nil:
		// postgres schemalog schema snapshot generator
		var schemaLogStore schemalog.Store
		var err error
		schemaLogStore, err = schemalogpg.NewStore(ctx, *cfg.SchemaLogStore)
		if err != nil {
			return nil, fmt.Errorf("create schema log postgres store: %w", err)
		}
		schemaLogStore = schemalog.NewStoreCache(schemaLogStore)
		return schemalogsnapshotgenerator.NewSnapshotGenerator(schemaLogStore, s.processRow), nil
	case cfg.DumpRestore != nil:
		// postgres pgdump/pgrestore schema snapshot generator
		return pgdumprestoregenerator.NewSnapshotGenerator(ctx, cfg.DumpRestore, pgdumprestoregenerator.WithLogger(logger))
	default:
		return nil, errSchemaSnapshotNotConfigured
	}
}
