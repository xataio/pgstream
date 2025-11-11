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
	rowsProcessor := adapter.NewProcessEventAdapter(p)

	var g generator.SnapshotGenerator
	var err error

	// postgres data snapshot generator layer
	opts := []pgsnapshotgenerator.Option{
		pgsnapshotgenerator.WithLogger(logger),
		pgsnapshotgenerator.WithProgressTracking(),
	}
	if instrumentation.IsEnabled() {
		opts = append(opts, pgsnapshotgenerator.WithInstrumentation(instrumentation))
	}
	g, err = pgsnapshotgenerator.NewSnapshotGenerator(ctx, &cfg.Generator, p, opts...)
	if err != nil {
		return nil, err
	}

	// snapshot table finder layer
	finderOpts := []pgtablefinder.Option{}
	if instrumentation.IsEnabled() {
		finderOpts = append(finderOpts, pgtablefinder.WithInstrumentation(instrumentation))
	}

	g, err = pgtablefinder.NewSnapshotSchemaTableFinder(ctx, cfg.Generator.URL, g, finderOpts...)
	if err != nil {
		return nil, err
	}

	// postgres schema snapshot generator layer
	g, err = newSchemaSnapshotGenerator(ctx, &cfg.Schema, g, rowsProcessor.ProcessRow, logger, instrumentation)
	if err != nil {
		return nil, err
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

func newSchemaSnapshotGenerator(ctx context.Context, cfg *SchemaSnapshotConfig, g generator.SnapshotGenerator, processRow snapshot.RowProcessor, logger loglib.Logger, instrumentation *otel.Instrumentation) (generator.SnapshotGenerator, error) {
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
		return schemalogsnapshotgenerator.NewSnapshotGenerator(
			schemaLogStore,
			processRow,
			schemalogsnapshotgenerator.WithSnapshotGenerator(g),
			schemalogsnapshotgenerator.WithLogger(logger)), nil
	case cfg.DumpRestore != nil:
		// postgres pgdump/pgrestore schema snapshot generator
		opts := []pgdumprestoregenerator.Option{
			pgdumprestoregenerator.WithLogger(logger),
			pgdumprestoregenerator.WithSnapshotGenerator(g),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, pgdumprestoregenerator.WithInstrumentation(instrumentation))
		}
		return pgdumprestoregenerator.NewSnapshotGenerator(ctx, cfg.DumpRestore, opts...)
	default:
		return nil, errSchemaSnapshotNotConfigured
	}
}
