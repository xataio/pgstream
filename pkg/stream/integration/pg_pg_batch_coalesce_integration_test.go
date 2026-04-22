// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

// Test_PostgresToPostgres_BatchCoalesce validates that the batch writer correctly
// coalesces consecutive same-table DML events into bulk SQL statements.
// It uses a large batch size so multiple events accumulate in a single batch.
func Test_PostgresToPostgres_BatchCoalesce(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener: testPostgresListenerCfg(),
		Processor: stream.ProcessorConfig{
			Postgres: &stream.PostgresProcessorConfig{
				BatchWriter: postgres.Config{
					URL: targetPGURL,
					BatchConfig: batch.Config{
						// Large batch size to accumulate multiple events per batch.
						// This forces the coalescing logic to run.
						MaxBatchSize: 500,
						BatchTimeout: 500 * time.Millisecond,
					},
					RetryPolicy: backoff.Config{
						DisableRetries: true,
					},
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	testTable := "pg2pg_batch_coalesce_test"

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// Step 1: Create the table
	execQuery(t, ctx, fmt.Sprintf(
		"CREATE TABLE %s (id serial PRIMARY KEY, name text, value int)", testTable))
	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	require.Eventually(t, func() bool {
		cols := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		return len(cols) == 3
	}, 20*time.Second, 200*time.Millisecond, "table schema not replicated")

	// Step 2: Bulk insert — 200 rows in a single transaction so they arrive
	// as a contiguous run of "I" events in one batch.
	numRows := 200
	execQuery(t, ctx, buildBulkInsert(testTable, numRows))

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s", testTable))
		if err != nil {
			return false
		}
		return count == numRows
	}, 30*time.Second, 500*time.Millisecond, "bulk insert rows not replicated")

	// Verify data integrity — spot-check first and last rows
	require.Eventually(t, func() bool {
		var name string
		var value int
		err := targetConn.QueryRow(ctx, []any{&name, &value},
			fmt.Sprintf("SELECT name, value FROM %s WHERE id = 1", testTable))
		if err != nil {
			return false
		}
		return name == "row_1" && value == 1
	}, 10*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		var name string
		var value int
		err := targetConn.QueryRow(ctx, []any{&name, &value},
			fmt.Sprintf("SELECT name, value FROM %s WHERE id = %d", testTable, numRows))
		if err != nil {
			return false
		}
		return name == fmt.Sprintf("row_%d", numRows) && value == numRows
	}, 10*time.Second, 200*time.Millisecond)

	// Step 3: Bulk delete — delete 100 rows in a single transaction.
	// These should coalesce into a bulk DELETE ... WHERE id = ANY($1::int4[]).
	deleteCount := 100
	execQuery(t, ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE id <= %d", testTable, deleteCount))

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s", testTable))
		if err != nil {
			return false
		}
		return count == numRows-deleteCount
	}, 30*time.Second, 500*time.Millisecond, "bulk delete not replicated")

	// Verify deleted rows are gone and remaining rows are intact
	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s WHERE id <= %d", testTable, deleteCount))
		if err != nil {
			return false
		}
		return count == 0
	}, 10*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		var minID int
		err := targetConn.QueryRow(ctx, []any{&minID},
			fmt.Sprintf("SELECT min(id) FROM %s", testTable))
		if err != nil {
			return false
		}
		return minID == deleteCount+1
	}, 10*time.Second, 200*time.Millisecond)

	// Step 4: Bulk update — update remaining rows. Updates are not coalesced
	// (handled individually) so this tests the non-coalesced path in a batch.
	execQuery(t, ctx, fmt.Sprintf(
		"UPDATE %s SET name = 'updated_' || id::text", testTable))

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s WHERE name LIKE 'updated_%%'", testTable))
		if err != nil {
			return false
		}
		return count == numRows-deleteCount
	}, 30*time.Second, 500*time.Millisecond, "bulk update not replicated")

	// Step 5: Mixed operations — insert + update + delete interleaved.
	// This tests that run boundaries are correctly flushed when the action changes.
	execQuery(t, ctx, fmt.Sprintf(
		"INSERT INTO %s(name, value) VALUES('mixed_1', 1000), ('mixed_2', 2000)", testTable))
	execQuery(t, ctx, fmt.Sprintf(
		"UPDATE %s SET value = 9999 WHERE name = 'mixed_1'", testTable))
	execQuery(t, ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE name = 'mixed_2'", testTable))

	require.Eventually(t, func() bool {
		var name string
		var value int
		err := targetConn.QueryRow(ctx, []any{&name, &value},
			fmt.Sprintf("SELECT name, value FROM %s WHERE name = 'mixed_1'", testTable))
		if err != nil {
			return false
		}
		return value == 9999
	}, 20*time.Second, 500*time.Millisecond, "mixed insert+update not replicated")

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s WHERE name = 'mixed_2'", testTable))
		if err != nil {
			return false
		}
		return count == 0
	}, 20*time.Second, 500*time.Millisecond, "mixed delete not replicated")
}

// Test_PostgresToPostgres_BatchCoalesce_WithCompositeKey validates coalescing
// for tables with composite primary keys.
func Test_PostgresToPostgres_BatchCoalesce_WithCompositeKey(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener: testPostgresListenerCfg(),
		Processor: stream.ProcessorConfig{
			Postgres: &stream.PostgresProcessorConfig{
				BatchWriter: postgres.Config{
					URL: targetPGURL,
					BatchConfig: batch.Config{
						MaxBatchSize: 500,
						BatchTimeout: 500 * time.Millisecond,
					},
					RetryPolicy: backoff.Config{
						DisableRetries: true,
					},
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	testTable := "pg2pg_batch_coalesce_composite_test"

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// Create table with composite primary key
	execQuery(t, ctx, fmt.Sprintf(
		"CREATE TABLE %s (tenant_id int, item_id int, name text, PRIMARY KEY(tenant_id, item_id))",
		testTable))
	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	require.Eventually(t, func() bool {
		cols := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		return len(cols) == 3
	}, 20*time.Second, 200*time.Millisecond, "table schema not replicated")

	// Bulk insert rows with composite key
	numRows := 50
	execQuery(t, ctx, buildCompositeKeyBulkInsert(testTable, numRows))

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s", testTable))
		if err != nil {
			return false
		}
		return count == numRows
	}, 30*time.Second, 500*time.Millisecond, "composite key bulk insert not replicated")

	// Bulk delete half the rows — these use composite PK IN tuples path
	execQuery(t, ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE item_id <= %d", testTable, numRows/2))

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s", testTable))
		if err != nil {
			return false
		}
		return count == numRows/2
	}, 30*time.Second, 500*time.Millisecond, "composite key bulk delete not replicated")
}

// Test_PostgresToPostgres_BatchCoalesce_OnConflict validates coalesced inserts
// with ON CONFLICT DO NOTHING.
func Test_PostgresToPostgres_BatchCoalesce_OnConflict(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener: testPostgresListenerCfg(),
		Processor: stream.ProcessorConfig{
			Postgres: &stream.PostgresProcessorConfig{
				BatchWriter: postgres.Config{
					URL:              targetPGURL,
					OnConflictAction: "nothing",
					BatchConfig: batch.Config{
						MaxBatchSize: 500,
						BatchTimeout: 500 * time.Millisecond,
					},
					RetryPolicy: backoff.Config{
						DisableRetries: true,
					},
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	testTable := "pg2pg_batch_coalesce_conflict_test"

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// Create table
	execQuery(t, ctx, fmt.Sprintf(
		"CREATE TABLE %s (id serial PRIMARY KEY, name text)", testTable))
	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	require.Eventually(t, func() bool {
		cols := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		return len(cols) == 2
	}, 20*time.Second, 200*time.Millisecond)

	// Insert rows
	numRows := 50
	execQuery(t, ctx, buildSimpleBulkInsert(testTable, numRows))

	require.Eventually(t, func() bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count},
			fmt.Sprintf("SELECT count(*) FROM %s", testTable))
		if err != nil {
			return false
		}
		return count == numRows
	}, 30*time.Second, 500*time.Millisecond, "on-conflict insert rows not replicated")
}

func buildBulkInsert(table string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s(name, value) VALUES", table)
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "('row_%d', %d)", i, i)
	}
	return b.String()
}

func buildSimpleBulkInsert(table string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s(name) VALUES", table)
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "('row_%d')", i)
	}
	return b.String()
}

func buildCompositeKeyBulkInsert(table string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s(tenant_id, item_id, name) VALUES", table)
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(1, %d, 'item_%d')", i, i)
	}
	return b.String()
}
