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

// Regression for #1211: binary COPY of a geometry column
// failed with "Invalid endian flag value encountered".
// Rows are read as the snapshot reads them, so the
// value shapes are the ones the writer really gets.
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
	// One spatial column puts every column on the text encoder.
	// The non-spatial ones are here to cover that.
	execQuery(t, ctx, conn, fmt.Sprintf(`CREATE TABLE %s(
		id         integer PRIMARY KEY,
		point      geometry(Point, 4326) NOT NULL,
		shape      geometry,
		place      geography(Point, 4326),
		labels     text[],
		notes      text,
		created_at timestamptz,
		amount     numeric(12,4),
		payload    bytea,
		uid        uuid,
		doc        jsonb,
		span       int4range,
		booked     tstzrange,
		flags      boolean[],
		duration   interval)`, sourceTable))
	// row 2 holds what breaks a naive text COPY:
	// the structural bytes, and an infinite timestamp
	execQuery(t, ctx, conn, fmt.Sprintf(`INSERT INTO %s VALUES
		(1, ST_SetSRID(ST_MakePoint(-0.12, 51.5), 4326),
		    ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
		    ST_SetSRID(ST_MakePoint(2.35, 48.85), 4326)::geography,
		    ARRAY['london', 'uk'], 'plain note',
		    '2024-03-01 12:34:56.789+00', 1234.5678, '\x00ff10'::bytea,
		    '0b7f1d64-5e2a-4c3b-9f8e-1a2b3c4d5e6f', '{"a": [1, 2], "b": null}',
		    '[1,10)', '[2024-01-01 00:00:00+00,2024-02-01 00:00:00+00)',
		    ARRAY[true, false], '1 day 02:03:04'),
		(2, ST_SetSRID(ST_MakePoint(13.4, 52.52), 4326),
		    ST_GeomFromText('LINESTRING(0 0, 2 2)'),
		    NULL,
		    ARRAY[]::text[], E'back\\slash\ttab\nnewline \\. {not,an,array}',
		    'infinity', -0.0001, ''::bytea,
		    '00000000-0000-0000-0000-000000000000', '"just a string"',
		    'empty', 'empty', ARRAY[]::boolean[], '-00:00:01'),
		(3, ST_SetSRID(ST_MakePoint(0, 0), 4326), NULL, NULL, NULL, NULL,
		    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`, sourceTable))
	execQuery(t, ctx, conn, fmt.Sprintf("CREATE TABLE %s(LIKE %s INCLUDING ALL)", targetTable, sourceTable))

	events := snapshotEvents(t, ctx, conn, "public", sourceTable, targetTable)
	require.Len(t, events, 3)
	// the writer gets hex EWKB text, not bytes
	require.Equal(t, "geometry", columnType(t, events[0], "point"))
	require.IsType(t, "", columnValue(t, events[0], "point"))

	writer, err := NewBulkIngestWriter(ctx, &Config{
		URL: pgURL,
		BatchConfig: batch.Config{
			BatchTimeout: time.Second,
			MaxBatchSize: 10,
		},
		// the default policy retries forever, hanging a regression
		RetryPolicy: backoff.Config{DisableRetries: true},
	}, WithLogger(loglib.NewNoopLogger()))
	require.NoError(t, err)

	for _, e := range events {
		require.NoError(t, writer.ProcessWALEvent(ctx, e))
	}
	// Close flushes the pending batch
	require.NoError(t, writer.Close())

	// canonical text, so PostGIS rendering cannot matter
	const columns = `id, ST_AsText(point), ST_AsEWKT(shape), ST_AsText(place::geometry),
		labels::text, notes, created_at::text, amount::text, payload::text, uid::text,
		doc::text, span::text, booked::text, flags::text, duration::text`
	want := fetchRows(t, ctx, conn, fmt.Sprintf("SELECT %s FROM %s ORDER BY id", columns, sourceTable))
	got := fetchRows(t, ctx, conn, fmt.Sprintf("SELECT %s FROM %s ORDER BY id", columns, targetTable))
	require.Equal(t, want, got)
}

// snapshotEvents reads rows the way the snapshot generator does,
// so the events carry the value shapes the writer receives.
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
	// the catalog query below needs the connection back
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

// fetchRows renders a NULL distinguishably from an empty value.
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
