// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// TestWAL2JSONCheck_Run_Integration exercises the temporary-slot probe against
// real Postgres servers: one that ships wal2json (debezium image) and one that
// does not (pgvector image with logical WAL enabled).
func TestWAL2JSONCheck_Run_Integration(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	t.Run("wal2json present - probe succeeds and leaves no replication slot", func(t *testing.T) {
		var pgURL string
		// debezium/postgres:14-alpine ships wal2json.so and defaults to
		// wal_level=logical (the 17-alpine variant does not bundle wal2json).
		cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres14)
		require.NoError(t, err)
		defer cleanup()

		check := &WAL2JSONCheck{Source: func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConn(ctx, pgURL)
		}}

		findings, err := check.Run(ctx)
		require.NoError(t, err)
		require.Empty(t, findings, "wal2json is present, no finding expected")

		// The probe must not leak a replication slot (it would otherwise count
		// against the replication_slot_headroom check that runs after it).
		adminConn, err := pglib.NewConn(ctx, pgURL)
		require.NoError(t, err)
		defer adminConn.Close(ctx)
		var slots int
		require.NoError(t, adminConn.QueryRow(ctx, []any{&slots},
			"SELECT count(*)::int FROM pg_replication_slots"))
		require.Equal(t, 0, slots, "temporary probe slot must be dropped")
	})

	t.Run("wal2json absent with wal_level=logical - reports missing finding", func(t *testing.T) {
		var pgURL string
		// pgvector image does not ship wal2json; the config enables logical WAL
		// so the probe reaches the plugin-load stage and fails on the missing lib.
		cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.PgvectorPostgres17, "testdata/wal_level_logical.conf")
		require.NoError(t, err)
		defer cleanup()

		check := &WAL2JSONCheck{Source: func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConn(ctx, pgURL)
		}}

		findings, err := check.Run(ctx)
		require.NoError(t, err)
		require.Len(t, findings, 1)
		require.Contains(t, findings[0].Message, "wal2json output plugin not available")
	})
}

// TestReplicaIdentityCheck_Run_Integration_PluginScoping proves end-to-end that
// a table filtered out by the wal2json plugin options is not flagged for a
// missing replica identity, while an in-scope table with the same problem is.
func TestReplicaIdentityCheck_Run_Integration_PluginScoping(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	adminConn, err := pglib.NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer adminConn.Close(ctx)

	// public.typeorm_metadata: no PK, default replica identity -> a finding
	// unless scoped out. labs.staff_member_onboarding: same problem, in scope.
	_, err = adminConn.Exec(ctx, `CREATE TABLE public.typeorm_metadata (id int, name text)`)
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, `CREATE SCHEMA labs`)
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, `CREATE TABLE labs.staff_member_onboarding (id int, name text)`)
	require.NoError(t, err)

	source := func(ctx context.Context) (pglib.Querier, error) {
		return pglib.NewConn(ctx, pgURL)
	}

	t.Run("filter_tables public.* scopes out public tables", func(t *testing.T) {
		cfg := &stream.Config{Listener: stream.ListenerConfig{Postgres: &stream.PostgresListenerConfig{
			Replication: pgreplication.Config{
				PluginArguments: pgreplication.PluginArguments{FilterTables: "public.*"},
			},
		}}}
		check := &ReplicaIdentityCheck{Source: source, Selection: cfg.ReplicationTableSelection()}

		findings, err := check.Run(ctx)
		require.NoError(t, err)

		joined := ""
		for _, f := range findings {
			joined += f.Message + "\n"
		}
		require.Contains(t, joined, `"labs"."staff_member_onboarding"`)
		require.NotContains(t, joined, "typeorm_metadata", "public.* is filtered out at the plugin level")
	})

	t.Run("no scoping flags the public table too", func(t *testing.T) {
		check := &ReplicaIdentityCheck{Source: source, Selection: (&stream.Config{}).ReplicationTableSelection()}

		findings, err := check.Run(ctx)
		require.NoError(t, err)

		joined := ""
		for _, f := range findings {
			joined += f.Message + "\n"
		}
		require.Contains(t, joined, `"public"."typeorm_metadata"`)
		require.Contains(t, joined, `"labs"."staff_member_onboarding"`)
	})
}
