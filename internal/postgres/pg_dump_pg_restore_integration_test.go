// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
)

func Test_pgdump_pgrestore(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var sourcePGURL, targetPGURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &sourcePGURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	cleanup2, err := testcontainers.SetupPostgresContainer(ctx, &targetPGURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup2()

	sourceConn, err := NewConn(ctx, sourcePGURL)
	require.NoError(t, err)
	targetConn, err := NewConn(ctx, targetPGURL)
	require.NoError(t, err)

	testSchema := "test_schema"
	testTable1 := "test_table_1"
	testTable2 := "test_table_2"

	// create schema + tables
	_, err = sourceConn.Exec(ctx, fmt.Sprintf("create schema %s", testSchema))
	require.NoError(t, err)
	_, err = sourceConn.Exec(ctx, fmt.Sprintf("create table %s.%s(id serial primary key, name text)", testSchema, testTable1))
	require.NoError(t, err)
	_, err = sourceConn.Exec(ctx, fmt.Sprintf("create table %s.%s(id serial primary key, name text)", testSchema, testTable2))
	require.NoError(t, err)
	// insert data
	_, err = sourceConn.Exec(ctx, fmt.Sprintf("insert into %s.%s(name) values('a'),('b'),('c')", testSchema, testTable1))
	require.NoError(t, err)

	run := func(t *testing.T, format string) {
		pgdumpOpts := PGDumpOptions{
			ConnectionString: sourcePGURL,
			Format:           format,
			SchemaOnly:       true,
			Schemas:          []string{testSchema},
			ExcludeTables:    []string{testSchema + "." + testTable2},
		}

		dump, err := RunPGDump(pgdumpOpts)
		require.NoError(t, err)

		pgrestoreOpts := PGRestoreOptions{
			ConnectionString: targetPGURL,
			Format:           format,
			SchemaOnly:       true,
		}

		_, err = RunPGRestore(pgrestoreOpts, dump)
		require.NoError(t, err)

		// schema only pgdump, no data should be available but the schema and
		// selected table should exist.
		var count int
		err = targetConn.QueryRow(ctx, fmt.Sprintf("select count(*) from %s.%s", testSchema, testTable1)).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)
		// test table 2 should not exist
		err = targetConn.QueryRow(ctx, fmt.Sprintf("select count(*) from %s.%s", testSchema, testTable2)).Scan(&count)
		require.Error(t, err)
	}

	t.Run("custom pg_dump format", func(t *testing.T) {
		run(t, "c")
	})
	t.Run("plain pg_dump format", func(t *testing.T) {
		run(t, "p")
	})
}
