// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

// Test_SnapshotToPostgres_ObjectTypeFilter runs the object type filter against
// the output of a real pg_dump. Which TOC entries the filter drops is pinned by
// the unit tests in pgdumprestore (TestObjectTypeFilter_IsExcluded,
// TestParseDump_WithObjectTypeFilter and friends), but those work off a
// captured dump; this one proves the filtering still holds against a dump
// postgres produced just now, and that the surviving entries restore into a
// usable target.
//
// The snapshot runs synchronously, so every assertion below is exact: once
// stream.Snapshot returns, anything the restore was going to create has been
// created. Nothing here needs polling, and nothing needs to wait out a timeout
// to prove an absence.
func Test_SnapshotToPostgres_ObjectTypeFilter(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var sourcePGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &sourcePGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixtureSQL, err := os.ReadFile("testdata/object_types_fixture.sql")
	require.NoError(t, err)
	execQueryWithURL(t, ctx, sourcePGURL, string(fixtureSQL))

	includeTypes := []string{"tables", "sequences", "types"}
	tables := []string{"app.*", "analytics.*"}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshotAndFilter(sourcePGURL, targetPGURL, tables, includeTypes),
		Processor: testPostgresProcessorCfg(),
	}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// included categories reach the target
	require.True(t, tableExists(ctx, targetConn, "app", "users"))
	require.True(t, tableExists(ctx, targetConn, "app", "posts"))
	require.True(t, tableExists(ctx, targetConn, "app", "categories"))
	require.True(t, tableExists(ctx, targetConn, "analytics", "page_views"))
	require.True(t, tableExists(ctx, targetConn, "analytics", "daily_stats"))

	require.True(t, pgTypeExists(ctx, targetConn, "app", "status"))
	require.True(t, pgTypeExists(ctx, targetConn, "app", "address"))
	require.True(t, pgTypeExists(ctx, targetConn, "app", "email"))
	require.True(t, pgTypeExists(ctx, targetConn, "app", "positive_int"))

	require.True(t, sequenceExists(ctx, targetConn, "app", "invoice_number_seq"))

	// the tables that survived the filter are restored well enough to hold
	// their data, not just their DDL
	count, err := rowCount(ctx, targetConn, "app.users")
	require.NoError(t, err)
	require.Equal(t, 5, count)

	count, err = rowCount(ctx, targetConn, "app.posts")
	require.NoError(t, err)
	require.Equal(t, 6, count)

	// excluded categories do not
	require.False(t, functionExists(ctx, targetConn, "app", "slugify"))
	require.False(t, functionExists(ctx, targetConn, "app", "get_post_comment_count"))

	require.False(t, viewExists(ctx, targetConn, "app", "published_posts"))
	require.False(t, viewExists(ctx, targetConn, "app", "user_stats"))

	// primary key indexes come with their tables; other indexes do not
	require.False(t, indexExists(ctx, targetConn, "app", "idx_users_email"))
	require.False(t, indexExists(ctx, targetConn, "app", "idx_posts_slug"))

	require.False(t, matviewExists(ctx, targetConn, "analytics", "top_posts"))
}

// --- Catalog query helpers ---

func tableExists(ctx context.Context, conn pglib.Querier, schema, table string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, table)
	return err == nil && exists
}

func pgTypeExists(ctx context.Context, conn pglib.Querier, schema, typeName string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE n.nspname = $1 AND t.typname = $2
		)`,
		schema, typeName)
	return err == nil && exists
}

func sequenceExists(ctx context.Context, conn pglib.Querier, schema, seqName string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(SELECT 1 FROM information_schema.sequences WHERE sequence_schema = $1 AND sequence_name = $2)`,
		schema, seqName)
	return err == nil && exists
}

func functionExists(ctx context.Context, conn pglib.Querier, schema, funcName string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1 AND p.proname = $2
		)`,
		schema, funcName)
	return err == nil && exists
}

func viewExists(ctx context.Context, conn pglib.Querier, schema, viewName string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_schema = $1 AND table_name = $2)`,
		schema, viewName)
	return err == nil && exists
}

func indexExists(ctx context.Context, conn pglib.Querier, schema, indexName string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2)`,
		schema, indexName)
	return err == nil && exists
}

func matviewExists(ctx context.Context, conn pglib.Querier, schema, matviewName string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(
			SELECT 1 FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2
		)`,
		schema, matviewName)
	return err == nil && exists
}

func rowCount(ctx context.Context, conn pglib.Querier, qualifiedTable string) (int, error) {
	var count int
	err := conn.QueryRow(ctx, []any{&count}, "SELECT count(*) FROM "+qualifiedTable)
	return count, err
}
