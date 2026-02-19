// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"context"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	generatorinstrumentation "github.com/xataio/pgstream/pkg/snapshot/generator/instrumentation"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestoregenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	pgtablefinder "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/tablefinder"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	snapshotstoreinstrumentation "github.com/xataio/pgstream/pkg/snapshot/store/instrumentation"
	pgsnapshotstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
	"github.com/xataio/pgstream/pkg/wal/listener"
	listenersnapshot "github.com/xataio/pgstream/pkg/wal/listener/snapshot"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
)

// NewSnapshotGenerator builds a snapshot generator based on the
// configuration on input, adapting the process wal event to fit with the
// snapshot package implementation. It will add an activity recorder layer if
// configured which will keep track of snapshot requests and their status.
//
// ┌───────────────────────────────────────────────────┐
// │               Snapshot Generator                  │
// │ ┌─────────┐  ┌─────────┐  ┌────────┐  ┌─────────┐ │
// │ │         │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ │Snapshot │  │ Schema  │  │ Table  │  │  Data   │ │
// │ │Activity │─▶│Snapshot ├─▶│ Finder ├─▶│Snapshot │ │
// │ │Recorder │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ └─────────┘  └─────────┘  └────────┘  └─────────┘ │
// └───────────────────────────────────────────────────┘

func NewSnapshotGenerator(ctx context.Context, cfg *SnapshotListenerConfig, p listener.Processor, logger loglib.Logger, instrumentation *otel.Instrumentation) (listenersnapshot.Generator, error) {
	var g generator.SnapshotGenerator
	var err error

	// postgres data snapshot generator layer
	if cfg.Data != nil {
		opts := []pgsnapshotgenerator.Option{
			pgsnapshotgenerator.WithLogger(logger),
		}
		if !cfg.DisableProgressTracking {
			opts = append(opts, pgsnapshotgenerator.WithProgressTracking())
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, pgsnapshotgenerator.WithInstrumentation(instrumentation))
		}
		g, err = pgsnapshotgenerator.NewSnapshotGenerator(ctx, cfg.Data, p, opts...)
		if err != nil {
			return nil, err
		}

		// snapshot table finder layer
		finderOpts := []pgtablefinder.Option{}
		if instrumentation.IsEnabled() {
			finderOpts = append(finderOpts, pgtablefinder.WithInstrumentation(instrumentation))
		}

		g, err = pgtablefinder.NewSnapshotSchemaTableFinder(ctx, cfg.Data.URL, g, finderOpts...)
		if err != nil {
			return nil, err
		}
	}

	if cfg.Schema != nil {
		// postgres schema snapshot generator layer
		g, err = newSchemaSnapshotGenerator(ctx, cfg.Schema, g, p, logger, instrumentation, !cfg.DisableProgressTracking)
		if err != nil {
			return nil, err
		}
	}

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
		g = generator.NewSnapshotRecorder(&generator.Config{
			RepeatableSnapshots: cfg.Recorder.RepeatableSnapshots,
			SchemaWorkers:       cfg.Recorder.SchemaWorkers,
		}, snapshotStore, g)
	}

	if instrumentation.IsEnabled() {
		g, err = generatorinstrumentation.NewSnapshotGenerator(g, instrumentation)
		if err != nil {
			return nil, err
		}
	}

	return adapter.NewSnapshotGeneratorAdapter(&cfg.Adapter, g, adapter.WithLogger(logger)), nil
}

func newSchemaSnapshotGenerator(ctx context.Context, cfg *SchemaSnapshotConfig, g generator.SnapshotGenerator, processor listener.Processor, logger loglib.Logger, instrumentation *otel.Instrumentation, progressTracking bool) (generator.SnapshotGenerator, error) {
	// postgres pgdump schema snapshot generator
	opts := []pgdumprestoregenerator.Option{
		pgdumprestoregenerator.WithLogger(logger),
		pgdumprestoregenerator.WithSnapshotGenerator(g),
	}
	if progressTracking {
		opts = append(opts, pgdumprestoregenerator.WithProgressTracking(ctx))
	}
	if instrumentation.IsEnabled() {
		opts = append(opts, pgdumprestoregenerator.WithInstrumentation(instrumentation))
	}
	if cfg.DumpRestore.TargetPGURL == "" {
		// if no target postgres is provided, use WAL restore instead of
		// direct pgrestore
		opts = append(opts, pgdumprestoregenerator.WithRestoreToWAL(processor))
	}
	return pgdumprestoregenerator.NewSnapshotGenerator(ctx, cfg.DumpRestore, opts...)
}
