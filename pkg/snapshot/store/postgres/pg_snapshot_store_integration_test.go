// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

// TestStore_CreateSnapshotRequest_wideSchema is a regression test for the btree
// index row size limit: a schema with hundreds of tables used to overflow the
// unique index on the raw table_names array ("index row size N exceeds btree
// version 4 maximum 2704"), failing the request insert. The index now hashes
// the table names, so the insert must succeed regardless of table count.
func TestStore_CreateSnapshotRequest_wideSchema(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	store, err := New(ctx, pgurl)
	require.NoError(t, err)
	defer store.Close()

	req := wideSchemaRequest()

	// the fix: this insert must not trip the btree row size limit
	err = store.CreateSnapshotRequest(ctx, req)
	require.NoError(t, err)

	// the hash index must still enforce uniqueness for non-completed requests: a
	// second identical request violates the unique index and is rejected
	err = store.CreateSnapshotRequest(ctx, req)
	require.Error(t, err)
}

// TestStore_createTable_upgradeFromLegacyIndex covers the in-place migration
// from the old raw-array unique index to the hashed one. A deployment created
// before the fix already has schema_table_status_unique_index over the raw
// (schema_name, table_names); re-running createTable must drop it and recreate
// the hashed index, since CREATE UNIQUE INDEX IF NOT EXISTS would otherwise keep
// the broken definition in place and wide-schema inserts would keep failing.
func TestStore_createTable_upgradeFromLegacyIndex(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	s, err := New(ctx, pgurl)
	require.NoError(t, err)
	defer s.Close()

	// simulate a deployment created before the fix: replace the hashed index
	// with the legacy raw-array index.
	_, err = s.conn.Exec(ctx, fmt.Sprintf(`DROP INDEX %s.schema_table_status_unique_index`, store.SchemaName))
	require.NoError(t, err)
	_, err = s.conn.Exec(ctx, fmt.Sprintf(`CREATE UNIQUE INDEX schema_table_status_unique_index
	ON %s(schema_name,table_names) WHERE status != 'completed'`, snapshotsTable()))
	require.NoError(t, err)

	// sanity check: the legacy index reproduces the original bug
	err = s.CreateSnapshotRequest(ctx, wideSchemaRequest())
	require.Error(t, err)

	// re-running createTable performs the migration: drop legacy, create hashed
	require.NoError(t, s.createTable(ctx))

	// the wide-schema insert now succeeds against the migrated index
	require.NoError(t, s.CreateSnapshotRequest(ctx, wideSchemaRequest()))
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
		sum := md5.Sum([]byte(fmt.Sprintf("pgstream-wide-schema-table-%d", i)))
		tables = append(tables, fmt.Sprintf("t_%x", sum))
	}
	return &snapshot.Request{
		Schema: "wide_schema",
		Tables: tables,
	}
}
