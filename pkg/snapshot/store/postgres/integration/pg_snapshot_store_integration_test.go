// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
	pgstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
)

// Test_SnapshotStore_WideSchema is a regression test for the btree index row
// size limit: a schema with hundreds of tables used to overflow the unique
// index on the raw table_names array ("index row size N exceeds btree version 4
// maximum 2704"), failing the request insert. The index now hashes the table
// names, so the insert must succeed regardless of table count.
func Test_SnapshotStore_WideSchema(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	s, err := pgstore.New(ctx, pgurl)
	require.NoError(t, err)
	defer s.Close()

	req := wideSchemaRequest()

	// the fix: this insert must not trip the btree row size limit
	require.NoError(t, s.CreateSnapshotRequest(ctx, req))

	// the hash index must still enforce uniqueness for non-completed requests: a
	// second identical request violates the unique index and is rejected
	require.Error(t, s.CreateSnapshotRequest(ctx, req))
}

// Test_SnapshotStore_UpgradeFromLegacyIndex covers the in-place migration from
// the old raw-array unique index to the hashed one. A deployment created before
// the fix already has schema_table_status_unique_index over the raw
// (schema_name, table_names); constructing the store again must drop it and
// recreate the hashed index, since CREATE UNIQUE INDEX IF NOT EXISTS would
// otherwise keep the broken definition in place and wide-schema inserts would
// keep failing.
func Test_SnapshotStore_UpgradeFromLegacyIndex(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	s, err := pgstore.New(ctx, pgurl)
	require.NoError(t, err)
	defer s.Close()

	// simulate a deployment created before the fix: over a separate connection,
	// replace the hashed index with the legacy raw-array index.
	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	snapshotsTable := pglib.QuoteQualifiedIdentifier(store.SchemaName, store.TableName)
	_, err = conn.Exec(ctx, fmt.Sprintf(`DROP INDEX %s.schema_table_status_hash_unique_index`, store.SchemaName))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE UNIQUE INDEX schema_table_status_unique_index
	ON %s(schema_name,table_names) WHERE status != 'completed'`, snapshotsTable))
	require.NoError(t, err)

	// sanity check: the legacy index reproduces the original bug
	require.Error(t, s.CreateSnapshotRequest(ctx, wideSchemaRequest()))

	// constructing the store again runs createTable, which performs the
	// migration: drop legacy, create hashed
	migrated, err := pgstore.New(ctx, pgurl)
	require.NoError(t, err)
	defer migrated.Close()

	// the wide-schema insert now succeeds against the migrated index
	require.NoError(t, migrated.CreateSnapshotRequest(ctx, wideSchemaRequest()))
}

// Test_SnapshotStore_CommaInTableNameNoCollision verifies the digest does not
// collapse distinct arrays: a single table literally named "a,b" must not hash
// to the same value as the two-table array {"a","b"}, which would otherwise
// cause a spurious unique-constraint violation between unrelated requests.
func Test_SnapshotStore_CommaInTableNameNoCollision(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	s, err := pgstore.New(ctx, pgurl)
	require.NoError(t, err)
	defer s.Close()

	// a single table named "a,b" and the two tables "a","b" are different
	// requests and must both be accepted
	require.NoError(t, s.CreateSnapshotRequest(ctx, &snapshot.Request{Schema: "collision", Tables: []string{"a,b"}}))
	require.NoError(t, s.CreateSnapshotRequest(ctx, &snapshot.Request{Schema: "collision", Tables: []string{"a", "b"}}))

	// uniqueness is still enforced for a genuine duplicate
	require.Error(t, s.CreateSnapshotRequest(ctx, &snapshot.Request{Schema: "collision", Tables: []string{"a", "b"}}))
}

// Test_SnapshotStore_RepeatedInitIsIdempotent verifies that constructing the
// store repeatedly (as happens on every process startup) does not rebuild the
// index or brick initialisation, and that uniqueness stays enforced across
// re-initialisations.
func Test_SnapshotStore_RepeatedInitIsIdempotent(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	s, err := pgstore.New(ctx, pgurl)
	require.NoError(t, err)
	defer s.Close()

	req := wideSchemaRequest()
	require.NoError(t, s.CreateSnapshotRequest(ctx, req))

	// re-initialising must not fail even with an existing non-completed request
	// (a per-startup drop+recreate could momentarily lose the constraint or fail)
	for range 3 {
		reinit, err := pgstore.New(ctx, pgurl)
		require.NoError(t, err)
		reinit.Close()
	}

	// uniqueness is still enforced after the re-initialisations
	require.Error(t, s.CreateSnapshotRequest(ctx, req))
}

// wideSchemaRequest builds a request whose table_names array comfortably exceeds
// the 2704 byte btree tuple limit that the raw-array index hit around ~380
// tables, so it fails on the legacy index and succeeds on the hashed one. The
// names must be high-entropy: postgres pglz-compresses the index value before
// the size check, so repetitive names would compress under the limit and fail
// to reproduce the bug.
func wideSchemaRequest() *snapshot.Request {
	tables := make([]string, 0, 300)
	for i := range 300 {
		sum := sha256.Sum256([]byte(fmt.Sprintf("pgstream-wide-schema-table-%d", i)))
		tables = append(tables, fmt.Sprintf("t_%x", sum))
	}
	return &snapshot.Request{
		Schema: "wide_schema",
		Tables: tables,
	}
}
