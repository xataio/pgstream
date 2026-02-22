// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

func Test_PostgresToPostgres_ObjectTypeFilter(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spin up a separate source Postgres container
	var sourcePGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(ctx, &sourcePGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	// Load fixture SQL into the source database
	fixtureSQL, err := os.ReadFile("testdata/object_types_fixture.sql")
	require.NoError(t, err)
	execQueryWithURL(t, ctx, sourcePGURL, string(fixtureSQL))

	// Configure pgstream with object type filtering for both snapshot and DDL replication
	includeTypes := []string{"tables", "sequences", "types"}
	tables := []string{"app.*", "analytics.*"}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshotAndFilter(sourcePGURL, targetPGURL, tables, includeTypes),
		Processor: testPostgresProcessorCfg(withDDLObjectTypeFilter(includeTypes)),
	}

	initStream(t, ctx, sourcePGURL)
	runStream(t, ctx, cfg)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	sourceConn, err := pglib.NewConn(ctx, sourcePGURL)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)

	// =========================================================================
	// Part 1: Snapshot assertions
	// =========================================================================

	// --- Should exist on target (poll with timeout) ---

	// Tables
	require.Eventually(t, func() bool {
		return tableExists(ctx, targetConn, "app", "users") &&
			tableExists(ctx, targetConn, "app", "posts") &&
			tableExists(ctx, targetConn, "app", "categories") &&
			tableExists(ctx, targetConn, "analytics", "page_views") &&
			tableExists(ctx, targetConn, "analytics", "daily_stats")
	}, 30*time.Second, time.Second, "expected tables to exist on target after snapshot")

	// Types
	require.Eventually(t, func() bool {
		return pgTypeExists(ctx, targetConn, "app", "status") &&
			pgTypeExists(ctx, targetConn, "app", "address") &&
			pgTypeExists(ctx, targetConn, "app", "email") &&
			pgTypeExists(ctx, targetConn, "app", "positive_int")
	}, 20*time.Second, time.Second, "expected types to exist on target after snapshot")

	// Sequences
	require.Eventually(t, func() bool {
		return sequenceExists(ctx, targetConn, "app", "invoice_number_seq")
	}, 20*time.Second, time.Second, "expected sequence to exist on target after snapshot")

	// Data
	require.Eventually(t, func() bool {
		count, err := rowCount(ctx, targetConn, "app.users")
		return err == nil && count == 5
	}, 20*time.Second, time.Second, "expected 5 rows in app.users")

	require.Eventually(t, func() bool {
		count, err := rowCount(ctx, targetConn, "app.posts")
		return err == nil && count == 6
	}, 20*time.Second, time.Second, "expected 6 rows in app.posts")

	// --- Should NOT exist on target ---

	// Functions
	require.Never(t, func() bool {
		return functionExists(ctx, targetConn, "app", "slugify") ||
			functionExists(ctx, targetConn, "app", "get_post_comment_count")
	}, 5*time.Second, time.Second, "functions should not exist on target")

	// Views
	require.Never(t, func() bool {
		return viewExists(ctx, targetConn, "app", "published_posts") ||
			viewExists(ctx, targetConn, "app", "user_stats")
	}, 5*time.Second, time.Second, "views should not exist on target")

	// Non-PK indexes
	require.Never(t, func() bool {
		return indexExists(ctx, targetConn, "app", "idx_users_email") ||
			indexExists(ctx, targetConn, "app", "idx_posts_slug")
	}, 5*time.Second, time.Second, "non-PK indexes should not exist on target")

	// Materialized views
	require.Never(t, func() bool {
		return matviewExists(ctx, targetConn, "analytics", "top_posts")
	}, 5*time.Second, time.Second, "materialized views should not exist on target")

	// =========================================================================
	// Part 2: DDL replication assertions
	// =========================================================================

	// --- Should replicate ---

	// CREATE TABLE
	execQueryWithURL(t, ctx, sourcePGURL, "CREATE TABLE app.filter_test(id serial PRIMARY KEY, val text)")
	require.Eventually(t, func() bool {
		return tableExists(ctx, targetConn, "app", "filter_test")
	}, 20*time.Second, time.Second, "expected app.filter_test table to replicate")

	// ALTER TABLE ADD COLUMN
	execQueryWithURL(t, ctx, sourcePGURL, "ALTER TABLE app.filter_test ADD COLUMN extra int")
	require.Eventually(t, func() bool {
		return columnExists(ctx, targetConn, "app", "filter_test", "extra")
	}, 20*time.Second, time.Second, "expected extra column to replicate")

	// INSERT DATA
	execQueryWithURL(t, ctx, sourcePGURL, "INSERT INTO app.filter_test(val) VALUES('hello')")
	require.Eventually(t, func() bool {
		count, err := rowCount(ctx, targetConn, "app.filter_test")
		return err == nil && count == 1
	}, 20*time.Second, time.Second, "expected data to replicate")

	// CREATE TYPE
	execQueryWithURL(t, ctx, sourcePGURL, "CREATE TYPE app.priority AS ENUM ('low','medium','high')")
	require.Eventually(t, func() bool {
		return pgTypeExists(ctx, targetConn, "app", "priority")
	}, 20*time.Second, time.Second, "expected app.priority type to replicate")

	// --- Should NOT replicate ---

	// CREATE FUNCTION
	execQueryWithURL(t, ctx, sourcePGURL, `CREATE FUNCTION app.filter_test_fn() RETURNS text AS $$ SELECT 'test'::text; $$ LANGUAGE sql`)
	require.Never(t, func() bool {
		return functionExists(ctx, targetConn, "app", "filter_test_fn")
	}, 5*time.Second, time.Second, "function should not replicate")

	// CREATE VIEW
	execQueryWithURL(t, ctx, sourcePGURL, "CREATE VIEW app.filter_test_view AS SELECT id, val FROM app.filter_test")
	require.Never(t, func() bool {
		return viewExists(ctx, targetConn, "app", "filter_test_view")
	}, 5*time.Second, time.Second, "view should not replicate")

	// CREATE INDEX
	execQueryWithURL(t, ctx, sourcePGURL, "CREATE INDEX filter_test_idx ON app.filter_test(val)")
	require.Never(t, func() bool {
		return indexExists(ctx, targetConn, "app", "filter_test_idx")
	}, 5*time.Second, time.Second, "index should not replicate")
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

func columnExists(ctx context.Context, conn pglib.Querier, schema, table, column string) bool {
	var exists bool
	err := conn.QueryRow(ctx, []any{&exists},
		`SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
		)`,
		schema, table, column)
	return err == nil && exists
}

func rowCount(ctx context.Context, conn pglib.Querier, qualifiedTable string) (int, error) {
	var count int
	err := conn.QueryRow(ctx, []any{&count}, "SELECT count(*) FROM "+qualifiedTable)
	return count, err
}
