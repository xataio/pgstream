// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// Test_BulkIngestWriter_PostGISColumns copies rows of a table with PostGIS
// geometry and geography columns through the bulk ingest writer.
//
// Regression for #1211: pgx has no codec for the per-database OIDs of the
// PostGIS types, so the snapshot reads such a column as text — the hex EWKB
// string — and binary COPY sent those characters where the server expects the
// binary layout. PostGIS read the first character of the hex as the byte order
// flag and rejected every row with "Invalid endian flag value encountered".
//
// The source rows are read the way the snapshot reads them, through
// rows.Values() and the type mapper, so the test pins the value shape the
// writer actually receives rather than a hand-written approximation of it.
func Test_BulkIngestWriter_PostGISColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.PostgisPostgres17)
	require.NoError(t, err)
	defer cleanup()

	conn, err := pglib.NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	const (
		sourceTable = "postgis_source"
		targetTable = "postgis_target"
	)

	execQuery(t, ctx, conn, "CREATE EXTENSION IF NOT EXISTS postgis")
	execQuery(t, ctx, conn, fmt.Sprintf(`CREATE TABLE %s(
		id     integer PRIMARY KEY,
		point  geometry(Point, 4326) NOT NULL,
		shape  geometry,
		place  geography(Point, 4326),
		labels text[])`, sourceTable))
	execQuery(t, ctx, conn, fmt.Sprintf(`INSERT INTO %s(id, point, shape, place, labels) VALUES
		(1, ST_SetSRID(ST_MakePoint(-0.12, 51.5), 4326),
		    ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
		    ST_SetSRID(ST_MakePoint(2.35, 48.85), 4326)::geography,
		    ARRAY['london', 'uk']),
		(2, ST_SetSRID(ST_MakePoint(13.4, 52.52), 4326),
		    ST_GeomFromText('LINESTRING(0 0, 2 2)'),
		    NULL,
		    ARRAY[]::text[]),
		(3, ST_SetSRID(ST_MakePoint(0, 0), 4326), NULL, NULL, NULL)`, sourceTable))
	execQuery(t, ctx, conn, fmt.Sprintf("CREATE TABLE %s(LIKE %s INCLUDING ALL)", targetTable, sourceTable))

	events := snapshotEvents(t, ctx, conn, "public", sourceTable, targetTable)
	require.Len(t, events, 3)
	// the geometry value reaches the writer as the hex EWKB text, which is what
	// binary COPY could not deliver
	require.Equal(t, "geometry", columnType(t, events[0], "point"))
	require.IsType(t, "", columnValue(t, events[0], "point"))

	writer, err := NewBulkIngestWriter(ctx, &Config{
		URL: pgURL,
		BatchConfig: batch.Config{
			BatchTimeout: time.Second,
			MaxBatchSize: 10,
		},
		// a COPY the target rejects is retried forever under the default
		// policy, so a regression here would hang the test instead of failing
		RetryPolicy: backoff.Config{DisableRetries: true},
	}, WithLogger(loglib.NewNoopLogger()))
	require.NoError(t, err)

	for _, e := range events {
		require.NoError(t, writer.ProcessWALEvent(ctx, e))
	}
	// closing the writer flushes the pending batch
	require.NoError(t, writer.Close())

	// compare the canonical text form of both tables so the assertion does not
	// depend on how PostGIS renders the binary values
	const columns = `id, ST_AsText(point), ST_AsEWKT(shape), ST_AsText(place::geometry), labels::text`
	want := fetchRows(t, ctx, conn, fmt.Sprintf("SELECT %s FROM %s ORDER BY id", columns, sourceTable))
	got := fetchRows(t, ctx, conn, fmt.Sprintf("SELECT %s FROM %s ORDER BY id", columns, targetTable))
	require.Equal(t, want, got)
}

// snapshotEvents reads every row of the given table the way the postgres
// snapshot generator does — rows.Values() for the values, and the type mapper
// for the postgres type name of each column — and returns them as insert
// events addressed to targetTable.
func snapshotEvents(t *testing.T, ctx context.Context, conn pglib.Querier, schema, sourceTable, targetTable string) []*wal.Event {
	t.Helper()

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY id", sourceTable))
	require.NoError(t, err)

	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, 0, len(fieldDescriptions))
	columnOIDs := make([]uint32, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		columnNames = append(columnNames, fd.Name)
		columnOIDs = append(columnOIDs, fd.DataTypeOID)
	}

	rowValues := [][]any{}
	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)
		rowValues = append(rowValues, values)
	}
	require.NoError(t, rows.Err())
	// the type names are resolved with a catalog query, which needs the
	// connection back
	rows.Close()

	mapper := pglib.NewMapper(conn)
	columnTypes := make([]string, 0, len(columnOIDs))
	for _, oid := range columnOIDs {
		dataType, err := mapper.TypeForOID(ctx, oid)
		require.NoError(t, err)
		columnTypes = append(columnTypes, dataType)
	}

	events := make([]*wal.Event, 0, len(rowValues))
	for _, values := range rowValues {
		columns := make([]wal.Column, 0, len(values))
		for i, v := range values {
			columns = append(columns, wal.Column{Name: columnNames[i], Type: columnTypes[i], Value: v})
		}
		events = append(events, &wal.Event{
			Data: &wal.Data{
				Action:    "I",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Schema:    schema,
				Table:     targetTable,
				Columns:   columns,
			},
		})
	}

	return events
}

func columnType(t *testing.T, e *wal.Event, name string) string {
	t.Helper()
	for _, c := range e.Data.Columns {
		if c.Name == name {
			return c.Type
		}
	}
	t.Fatalf("column %s not found", name)
	return ""
}

func columnValue(t *testing.T, e *wal.Event, name string) any {
	t.Helper()
	for _, c := range e.Data.Columns {
		if c.Name == name {
			return c.Value
		}
	}
	t.Fatalf("column %s not found", name)
	return nil
}

func execQuery(t *testing.T, ctx context.Context, conn pglib.Querier, query string) {
	t.Helper()
	_, err := conn.Exec(ctx, query)
	require.NoError(t, err)
}

// fetchRows returns the rows of the given query as strings, with a NULL
// rendered as "<null>" so it stays distinguishable from an empty value.
func fetchRows(t *testing.T, ctx context.Context, conn pglib.Querier, query string) [][]string {
	t.Helper()

	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	out := [][]string{}
	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)
		row := make([]string, 0, len(values))
		for _, v := range values {
			if v == nil {
				row = append(row, "<null>")
				continue
			}
			row = append(row, fmt.Sprint(v))
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err())

	return out
}
