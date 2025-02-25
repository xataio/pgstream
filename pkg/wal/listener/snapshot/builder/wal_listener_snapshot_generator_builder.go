// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"context"
	"errors"
	"fmt"

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

func NewSnapshotGenerator(ctx context.Context, cfg *SnapshotListenerConfig, processEvent listener.ProcessWalEvent, logger loglib.Logger) (listenersnapshot.Generator, error) {
	processEventAdapter := adapter.NewProcessEventAdapter(processEvent)
	// postgres schema snapshot generator
	schemaSnapshotGenerator, err := newSchemaSnapshotGenerator(ctx, &cfg.Schema, processEventAdapter.ProcessRow, logger)
	if err != nil {
		return nil, err
	}

	// postgres data snapshot generator
	dataSnapshotGenerator, err := pgsnapshotgenerator.NewSnapshotGenerator(ctx, &cfg.Generator, processEventAdapter.ProcessRow, pgsnapshotgenerator.WithLogger(logger))
	if err != nil {
		return nil, err
	}

	var g generator.SnapshotGenerator
	// snapshot generator aggregator
	g = generator.NewAggregator([]generator.SnapshotGenerator{schemaSnapshotGenerator, dataSnapshotGenerator})

	// snapshot table finder layer
	g, err = pgtablefinder.NewSnapshotTableFinder(ctx, cfg.Generator.URL, g)
	if err != nil {
		return nil, err
	}

	if cfg.SnapshotStoreURL != "" {
		// snapshot activity recorder layer
		snapshotStore, err := pgsnapshotstore.New(ctx, cfg.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("create postgres snapshot store: %w", err)
		}
		g = generator.NewSnapshotRecorder(snapshotStore, g)
	}

	return adapter.NewSnapshotGeneratorAdapter(&cfg.Adapter, g, adapter.WithLogger(logger)), nil
}

func newSchemaSnapshotGenerator(ctx context.Context, cfg *SchemaSnapshotConfig, processRow snapshot.RowProcessor, logger loglib.Logger) (generator.SnapshotGenerator, error) {
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
		return schemalogsnapshotgenerator.NewSnapshotGenerator(schemaLogStore, processRow), nil
	case cfg.DumpRestore != nil:
		// postgres pgdump/pgrestore schema snapshot generator
		return pgdumprestoregenerator.NewSnapshotGenerator(ctx, cfg.DumpRestore, pgdumprestoregenerator.WithLogger(logger))
	default:
		return nil, errSchemaSnapshotNotConfigured
	}
}
