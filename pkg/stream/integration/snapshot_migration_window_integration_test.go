// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	pgtablefinder "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/tablefinder"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

// migrationHookGenerator migrates, then delegates.
type migrationHookGenerator struct {
	wrapped generator.SnapshotGenerator
	migrate func()
}

func (g *migrationHookGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	g.migrate()
	return g.wrapped.CreateSnapshot(ctx, ss)
}

func (g *migrationHookGenerator) Close() error { return g.wrapped.Close() }

// migration lands after the dump
// chain assembled to place it
func Test_SnapshotToPostgres_MigrationBetweenSchemaAndDataSnapshot(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sourceURL string
	sourceCleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &sourceURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer sourceCleanup()

	var targetURL string
	targetCleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &targetURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer targetCleanup()

	testTable := fmt.Sprintf("migration_window_%d", time.Now().UnixNano())
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(
		`CREATE TABLE %s(id integer PRIMARY KEY, name text NOT NULL)`, testTable))
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (1, 'a'),(2, 'b')`, testTable))

	// runs after dump and restore
	migrated := false
	migrate := func() {
		execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN added_later text NOT NULL DEFAULT 'from_migration'`, testTable))
		migrated = true
	}

	writer, err := pgwriter.NewBatchWriter(ctx, &pgwriter.Config{
		URL:         targetURL,
		BatchConfig: batch.Config{MaxBatchSize: 1},
		RetryPolicy: backoff.Config{DisableRetries: true},
	})
	require.NoError(t, err)

	dataGenerator, err := pgsnapshotgenerator.NewSnapshotGenerator(ctx,
		&pgsnapshotgenerator.Config{URL: sourceURL}, writer)
	require.NoError(t, err)

	tableFinder, err := pgtablefinder.NewSnapshotSchemaTableFinder(ctx, sourceURL, dataGenerator)
	require.NoError(t, err)

	schemaGenerator, err := pgdumprestore.NewSnapshotGenerator(ctx,
		&pgdumprestore.Config{SourcePGURL: sourceURL, TargetPGURL: targetURL},
		pgdumprestore.WithLogger(testLogger()),
		pgdumprestore.WithSnapshotGenerator(&migrationHookGenerator{
			wrapped: tableFinder,
			migrate: migrate,
		}))
	require.NoError(t, err)
	defer schemaGenerator.Close()

	err = schemaGenerator.CreateSnapshot(ctx, &snapshot.Snapshot{
		SchemaTables: map[string][]string{"public": {testTable}},
	})
	require.NoError(t, err, "snapshot must survive a migration landing after the schema was dumped")
	require.True(t, migrated, "the migration hook never ran, so this test proves nothing")

	sourceConn, err := pglib.NewConn(ctx, sourceURL)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)
	targetConn, err := pglib.NewConn(ctx, targetURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// source has it, target doesn't
	sourceColumns := getInformationSchemaColumns(t, ctx, sourceConn, testTable)
	require.Len(t, sourceColumns, 3)
	targetColumns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
	require.ElementsMatch(t, []*informationSchemaColumn{
		{name: "id", dataType: "integer", isNullable: "NO"},
		{name: "name", dataType: "text", isNullable: "NO"},
	}, targetColumns, "the target must not have the migrated column, or the window under test did not happen")

	rows := getIDNameRows(t, ctx, targetConn, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", testTable))
	require.Equal(t, []idNameRow{{id: 1, name: "a"}, {id: 2, name: "b"}}, rows)
}
