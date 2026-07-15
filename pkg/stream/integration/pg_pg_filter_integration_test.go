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
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
)

// Test_PostgresToPostgres_SchemaOnlyTablesFilter verifies that DDL events for
// tables in the filter schema-only list are replicated while their data (DML)
// events are dropped, and that tables outside the list are unaffected.
func Test_PostgresToPostgres_SchemaOnlyTablesFilter(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	streamedTable := "pg2pg_filter_streamed_test"
	schemaOnlyTable := "pg2pg_filter_schema_only_test"

	cfg := &stream.Config{
		Listener: testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(withFilter(&filter.Config{
			SchemaOnlyTables: []string{schemaOnlyTable},
		})),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	defer execQuery(t, ctx, fmt.Sprintf("drop table if exists %s", streamedTable))
	defer execQuery(t, ctx, fmt.Sprintf("drop table if exists %s", schemaOnlyTable))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// DDL events pass the filter for both tables
	execQuery(t, ctx, fmt.Sprintf("create table %s(id serial primary key, name text)", streamedTable))
	execQuery(t, ctx, fmt.Sprintf("create table %s(id serial primary key, name text)", schemaOnlyTable))

	require.Eventually(t, func() bool {
		return len(getInformationSchemaColumns(t, ctx, targetConn, streamedTable)) == 2 &&
			len(getInformationSchemaColumns(t, ctx, targetConn, schemaOnlyTable)) == 2
	}, 20*time.Second, 200*time.Millisecond, "create table DDL not replicated")

	// insert into the schema-only table first, then into the streamed one:
	// events are processed in order, so once the streamed row lands the
	// schema-only insert has already been filtered out
	execQuery(t, ctx, fmt.Sprintf("insert into %s(name) values('a')", schemaOnlyTable))
	execQuery(t, ctx, fmt.Sprintf("insert into %s(name) values('b')", streamedTable))

	require.Eventually(t, func() bool {
		return len(getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s", streamedTable))) == 1
	}, 20*time.Second, 200*time.Millisecond, "streamed table insert not replicated")

	require.Empty(t, getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s", schemaOnlyTable)),
		"schema-only table data should not be replicated")

	// DDL events keep replicating for the schema-only table
	execQuery(t, ctx, fmt.Sprintf("alter table %s add column age int", schemaOnlyTable))

	require.Eventually(t, func() bool {
		return len(getInformationSchemaColumns(t, ctx, targetConn, schemaOnlyTable)) == 3
	}, 20*time.Second, 200*time.Millisecond, "alter table DDL not replicated for schema-only table")
}
