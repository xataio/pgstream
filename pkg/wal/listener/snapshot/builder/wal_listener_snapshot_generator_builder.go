// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"context"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemaloginstrumentation "github.com/xataio/pgstream/pkg/schemalog/instrumentation"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	generatorinstrumentation "github.com/xataio/pgstream/pkg/snapshot/generator/instrumentation"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestoregenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	schemalogsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/schemalog"
	pgtablefinder "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/tablefinder"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	snapshotstoreinstrumentation "github.com/xataio/pgstream/pkg/snapshot/store/instrumentation"
	pgsnapshotstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
	"github.com/xataio/pgstream/pkg/wal/listener"
	listenersnapshot "github.com/xataio/pgstream/pkg/wal/listener/snapshot"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
)

var errSchemaSnapshotNotConfigured = errors.New("no schema snapshot has been configured")

// NewSnapshotGenerator builds a snapshot generator based on the
// configuration on input, adapting the process wal event to fit with the
// snapshot package implementation. It will add an activity recorder layer if
// configured which will keep track of snapshot requests and their status.
//
// ┌───────────────────────────────────────────────────┐
// │               Snapshot Generator                  │
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

func NewSnapshotGenerator(ctx context.Context, cfg *SnapshotListenerConfig, processEvent listener.ProcessWalEvent, logger loglib.Logger, instrumentation *otel.Instrumentation) (listenersnapshot.Generator, error) {
	processEventAdapter := adapter.NewProcessEventAdapter(processEvent)
	// postgres schema snapshot generator
	schemaSnapshotGenerator, err := newSchemaSnapshotGenerator(ctx, &cfg.Schema, processEventAdapter.ProcessRow, logger, instrumentation)
	if err != nil {
		return nil, err
	}

	// postgres data snapshot generator
	var dataSnapshotGenerator generator.SnapshotGenerator
	opts := []pgsnapshotgenerator.Option{
		pgsnapshotgenerator.WithLogger(logger),
	}
	if instrumentation.IsEnabled() {
		opts = append(opts, pgsnapshotgenerator.WithInstrumentation(instrumentation))
	}
	dataSnapshotGenerator, err = pgsnapshotgenerator.NewSnapshotGenerator(ctx, &cfg.Generator, processEventAdapter.ProcessRow, opts...)
	if err != nil {
		return nil, err
	}
	// snapshot table finder layer
	finderOpts := []pgtablefinder.Option{}
	if instrumentation.IsEnabled() {
		finderOpts = append(finderOpts, pgtablefinder.WithInstrumentation(instrumentation))
	}
	dataSnapshotGenerator, err = pgtablefinder.NewSnapshotTableFinder(ctx, cfg.Generator.URL, dataSnapshotGenerator, finderOpts...)
	if err != nil {
		return nil, err
	}

	var g generator.SnapshotGenerator
	// snapshot generator aggregator
	g = generator.NewAggregator([]generator.SnapshotGenerator{schemaSnapshotGenerator, dataSnapshotGenerator})

	if cfg.Recorder != nil && cfg.Recorder.SnapshotStoreURL != "" {
		// snapshot activity recorder layer
		var snapshotStore snapshotstore.Store
		snapshotStore, err := pgsnapshotstore.New(ctx, cfg.Recorder.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("create postgres snapshot store: %w", err)
		}
		if instrumentation.IsEnabled() {
			snapshotStore = snapshotstoreinstrumentation.NewStore(snapshotStore, instrumentation)
		}
		g = generator.NewSnapshotRecorder(snapshotStore, g, cfg.Recorder.RepeatableSnapshots)
	}

	if instrumentation.IsEnabled() {
		g, err = generatorinstrumentation.NewSnapshotGenerator(g, instrumentation)
		if err != nil {
			return nil, err
		}
	}

	return adapter.NewSnapshotGeneratorAdapter(&cfg.Adapter, g, adapter.WithLogger(logger)), nil
}

func newSchemaSnapshotGenerator(ctx context.Context, cfg *SchemaSnapshotConfig, processRow snapshot.RowProcessor, logger loglib.Logger, instrumentation *otel.Instrumentation) (generator.SnapshotGenerator, error) {
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
		if instrumentation.IsEnabled() {
			schemaLogStore = schemaloginstrumentation.NewStore(schemaLogStore, instrumentation)
		}
		return schemalogsnapshotgenerator.NewSnapshotGenerator(schemaLogStore, processRow), nil
	case cfg.DumpRestore != nil:
		// postgres pgdump/pgrestore schema snapshot generator
		opts := []pgdumprestoregenerator.Option{
			pgdumprestoregenerator.WithLogger(logger),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, pgdumprestoregenerator.WithInstrumentation(instrumentation))
		}
		return pgdumprestoregenerator.NewSnapshotGenerator(ctx, cfg.DumpRestore, opts...)
	default:
		return nil, errSchemaSnapshotNotConfigured
	}
}
