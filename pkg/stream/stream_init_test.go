// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
)

var testPGURL string

func TestMain(m *testing.M) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") != "" {
		ctx := context.Background()
		cleanup, err := testcontainers.SetupPostgresContainer(ctx, &testPGURL, testcontainers.Postgres14, "../../pkg/stream/integration/config/postgresql.conf")
		if err != nil {
			panic(err)
		}
		defer cleanup()
	}

	os.Exit(m.Run())
}

func TestInit_Upgrade_WithV09xState(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Set up v0.9.x state
	conn, err := pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	setupV09xState(t, ctx, conn)

	// Run Init with Upgrade=true and MigrationsOnly=true (to avoid
	// needing wal2json for replication slot creation)
	err = Init(ctx, &InitConfig{
		PostgresURL:    testPGURL,
		Upgrade:        true,
		MigrationsOnly: true,
	})
	require.NoError(t, err)

	// Verify v0.9.x objects are removed
	assertObjectNotExists(t, ctx, conn, "table", "pgstream", "schema_log")
	assertObjectNotExists(t, ctx, conn, "table", "pgstream", "schema_migrations")

	// Verify v1.0 migration tables exist
	assertObjectExists(t, ctx, conn, "table", "pgstream", "schema_migrations_core")
}

func TestInit_Upgrade_NoV09xState(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Clean up any prior state
	conn, err := pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	cleanupAllState(t, ctx, conn)
	conn.Close(ctx)

	// Run Init with Upgrade=true on a clean DB
	err = Init(ctx, &InitConfig{
		PostgresURL:    testPGURL,
		Upgrade:        true,
		MigrationsOnly: true,
	})
	require.NoError(t, err)

	conn, err = pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Verify v1.0 state was created
	assertObjectExists(t, ctx, conn, "table", "pgstream", "schema_migrations_core")
}

func TestInit_Upgrade_Idempotent(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Clean up any prior state
	conn, err := pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	cleanupAllState(t, ctx, conn)
	conn.Close(ctx)

	cfg := &InitConfig{
		PostgresURL:    testPGURL,
		Upgrade:        true,
		MigrationsOnly: true,
	}

	// First run
	err = Init(ctx, cfg)
	require.NoError(t, err)

	// Second run — should be a no-op, no errors
	err = Init(ctx, cfg)
	require.NoError(t, err)

	conn, err = pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	assertObjectExists(t, ctx, conn, "table", "pgstream", "schema_migrations_core")
}

func TestInit_Upgrade_WithInjector(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Clean up any prior state and set up v0.9.x state
	conn, err := pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	cleanupAllState(t, ctx, conn)
	setupV09xState(t, ctx, conn)

	// Insert test data into table_ids (should be preserved across upgrade)
	_, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS pgstream.table_ids (id text PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO pgstream.table_ids (id) VALUES ('test-id') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	conn.Close(ctx)

	// Run Init with Upgrade and injector enabled
	err = Init(ctx, &InitConfig{
		PostgresURL:               testPGURL,
		Upgrade:                   true,
		MigrationsOnly:            true,
		InjectorMigrationsEnabled: true,
	})
	require.NoError(t, err)

	conn, err = pgx.Connect(ctx, testPGURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Verify v0.9.x objects are removed
	assertObjectNotExists(t, ctx, conn, "table", "pgstream", "schema_log")
	assertObjectNotExists(t, ctx, conn, "table", "pgstream", "schema_migrations")

	// Verify injector migration table exists
	assertObjectExists(t, ctx, conn, "table", "pgstream", "schema_migrations_injector")

	// Verify table_ids data was preserved
	var id string
	err = conn.QueryRow(ctx, `SELECT id FROM pgstream.table_ids WHERE id = 'test-id'`).Scan(&id)
	require.NoError(t, err)
	require.Equal(t, "test-id", id)
}

func TestWithUpgrade(t *testing.T) {
	t.Parallel()

	cfg := &InitConfig{}
	WithUpgrade()(cfg)
	require.True(t, cfg.Upgrade)
}

// setupV09xState creates the database objects that v0.9.x would have created.
func setupV09xState(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()

	statements := []string{
		"CREATE SCHEMA IF NOT EXISTS pgstream",
		"CREATE TABLE IF NOT EXISTS pgstream.schema_migrations (version bigint PRIMARY KEY, dirty boolean NOT NULL)",
		"CREATE TABLE IF NOT EXISTS pgstream.schema_log (id serial PRIMARY KEY, version text)",
		`CREATE OR REPLACE FUNCTION pgstream.log_schema() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql`,
		`CREATE OR REPLACE FUNCTION pgstream.get_schema(p_name text) RETURNS text AS $$ BEGIN RETURN ''; END; $$ LANGUAGE plpgsql`,
		`CREATE OR REPLACE FUNCTION pgstream.refresh_schema() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql`,
		`DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_create_alter_table`,
		`CREATE EVENT TRIGGER pgstream_log_schema_create_alter_table ON ddl_command_end EXECUTE FUNCTION pgstream.log_schema()`,
		`DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_drop_schema_table`,
		`CREATE EVENT TRIGGER pgstream_log_schema_drop_schema_table ON sql_drop EXECUTE FUNCTION pgstream.log_schema()`,
	}

	for _, stmt := range statements {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err, "failed to execute: %s", stmt)
	}
}

// cleanupAllState drops the pgstream schema and any event triggers.
func cleanupAllState(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()

	statements := []string{
		"DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_create_alter_table",
		"DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_drop_schema_table",
		"DROP SCHEMA IF EXISTS pgstream CASCADE",
	}
	for _, stmt := range statements {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err)
	}
}

func assertObjectExists(t *testing.T, ctx context.Context, conn *pgx.Conn, objectType, schema, name string) {
	t.Helper()

	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, name).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "%s %s.%s should exist", objectType, schema, name)
}

func assertObjectNotExists(t *testing.T, ctx context.Context, conn *pgx.Conn, objectType, schema, name string) {
	t.Helper()

	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, name).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "%s %s.%s should not exist", objectType, schema, name)
}
