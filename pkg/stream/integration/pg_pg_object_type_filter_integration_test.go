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

	// The catalog helpers return their error rather than folding it into the
	// boolean. An exclusion assertion written over `err == nil && exists`
	// passes just as happily when the query itself failed, which is exactly
	// the case these assertions exist to catch.
	mustExist := func(exists bool, err error) {
		t.Helper()
		require.NoError(t, err)
		require.True(t, exists, "expected on the target after the snapshot")
	}
	mustNotExist := func(exists bool, err error) {
		t.Helper()
		require.NoError(t, err)
		require.False(t, exists, "expected to be dropped by the object type filter")
	}

	// included categories reach the target
	mustExist(tableExists(ctx, targetConn, "app", "users"))
	mustExist(tableExists(ctx, targetConn, "app", "posts"))
	mustExist(tableExists(ctx, targetConn, "app", "categories"))
	mustExist(tableExists(ctx, targetConn, "analytics", "page_views"))
	mustExist(tableExists(ctx, targetConn, "analytics", "daily_stats"))

	mustExist(pgTypeExists(ctx, targetConn, "app", "status"))
	mustExist(pgTypeExists(ctx, targetConn, "app", "address"))
	mustExist(pgTypeExists(ctx, targetConn, "app", "email"))
	mustExist(pgTypeExists(ctx, targetConn, "app", "positive_int"))

	mustExist(sequenceExists(ctx, targetConn, "app", "invoice_number_seq"))

	// the tables that survived the filter are restored well enough to hold
	// their data, not just their DDL
	count, err := rowCount(ctx, targetConn, "app.users")
	require.NoError(t, err)
	require.Equal(t, 5, count)

	count, err = rowCount(ctx, targetConn, "app.posts")
	require.NoError(t, err)
	require.Equal(t, 6, count)

	// excluded categories do not
	mustNotExist(functionExists(ctx, targetConn, "app", "slugify"))
	mustNotExist(functionExists(ctx, targetConn, "app", "get_post_comment_count"))

	mustNotExist(viewExists(ctx, targetConn, "app", "published_posts"))
	mustNotExist(viewExists(ctx, targetConn, "app", "user_stats"))

	// primary key indexes come with their tables; other indexes do not
	mustNotExist(indexExists(ctx, targetConn, "app", "idx_users_email"))
	mustNotExist(indexExists(ctx, targetConn, "app", "idx_posts_slug"))

	mustNotExist(matviewExists(ctx, targetConn, "analytics", "top_posts"))
}

// --- Catalog query helpers ---

// catalogExists runs an EXISTS probe and hands back the query error separately
// from the answer, so a caller asserting an absence cannot mistake a failed
// query for a missing object.
func catalogExists(ctx context.Context, conn pglib.Querier, query string, args ...any) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists}, query, args...)
	return exists, err
}

func tableExists(ctx context.Context, conn pglib.Querier, schema, table string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, table)
}

func pgTypeExists(ctx context.Context, conn pglib.Querier, schema, typeName string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE n.nspname = $1 AND t.typname = $2
		)`,
		schema, typeName)
}

func sequenceExists(ctx context.Context, conn pglib.Querier, schema, seqName string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(SELECT 1 FROM information_schema.sequences WHERE sequence_schema = $1 AND sequence_name = $2)`,
		schema, seqName)
}

func functionExists(ctx context.Context, conn pglib.Querier, schema, funcName string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1 AND p.proname = $2
		)`,
		schema, funcName)
}

func viewExists(ctx context.Context, conn pglib.Querier, schema, viewName string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_schema = $1 AND table_name = $2)`,
		schema, viewName)
}

func indexExists(ctx context.Context, conn pglib.Querier, schema, indexName string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2)`,
		schema, indexName)
}

func matviewExists(ctx context.Context, conn pglib.Querier, schema, matviewName string) (bool, error) {
	return catalogExists(ctx, conn,
		`SELECT EXISTS(
			SELECT 1 FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2
		)`,
		schema, matviewName)
}

func rowCount(ctx context.Context, conn pglib.Querier, qualifiedTable string) (int, error) {
	var count int
	err := conn.QueryRow(ctx, []any{&count}, "SELECT count(*) FROM "+qualifiedTable)
	return count, err
}
