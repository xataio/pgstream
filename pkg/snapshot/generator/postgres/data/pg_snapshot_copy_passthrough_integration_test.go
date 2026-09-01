// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal"
)

func Test_PostgresSnapshotGenerator_copyPassthrough(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sourceURL, targetURL string
	sourceCleanup, err := testcontainers.SetupPostgresContainer(ctx, &sourceURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer sourceCleanup()
	targetCleanup, err := testcontainers.SetupPostgresContainer(ctx, &targetURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer targetCleanup()

	const testTable = "copy_passthrough_test"
	schema := fmt.Sprintf(`CREATE TABLE %s (
		id      int primary key,
		label   text,
		amount  numeric,
		ts      timestamptz,
		flags   bool[],
		payload bytea
	)`, testTable)
	execQuery(t, ctx, sourceURL, schema)
	// stands in for the schema snapshot
	execQuery(t, ctx, targetURL, schema)

	execQuery(t, ctx, sourceURL, fmt.Sprintf(`INSERT INTO %s VALUES
		(1, 'plain',                 '10.25',  '2024-01-02 03:04:05+00', '{t,f}', '\x00ff'),
		(2, E'tab\there',            '-0.001', '1999-12-31 23:59:59+00', '{}',    '\x'),
		(3, E'newline\nand\rreturn', '0',      'infinity',               NULL,    NULL),
		(4, E'back\\slash',          NULL,     '-infinity',              '{t}',   '\xdeadbeef'),
		(5, '',                      '1e10',   '2024-06-01 12:00:00+00', '{f,f}', '\x0a0d09')`,
		testTable))

	// any event means it decoded instead
	generator, err := NewSnapshotGenerator(ctx, &Config{
		URL:             sourceURL,
		CopyPassthrough: &CopyPassthroughConfig{TargetURL: targetURL},
	}, &failingProcessor{t: t})
	require.NoError(t, err)
	defer generator.Close()

	require.NoError(t, generator.CreateSnapshot(ctx, &snapshot.Snapshot{
		SchemaTables: map[string][]string{"public": {testTable}},
	}))

	require.Equal(t,
		readRowsAsText(t, ctx, sourceURL, testTable),
		readRowsAsText(t, ctx, targetURL, testTable))
}

// pins what binary format relies on
func Test_PostgresSnapshotGenerator_copyPassthrough_userDefinedTypes(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sourceURL, targetURL string
	sourceCleanup, err := testcontainers.SetupPostgresContainer(ctx, &sourceURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer sourceCleanup()
	targetCleanup, err := testcontainers.SetupPostgresContainer(ctx, &targetURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer targetCleanup()

	const testTable = "copy_passthrough_udt_test"
	schema := fmt.Sprintf(`
		CREATE TYPE mood AS ENUM ('sad','ok','happy');
		CREATE TYPE addr AS (street text, num int);
		CREATE DOMAIN pos AS int;
		CREATE DOMAIN moodd AS mood;
		CREATE TYPE numrng AS RANGE (subtype = numeric);
		CREATE TABLE %s (
			id        int primary key,
			c_enum    mood,
			c_enumarr mood[],
			c_comp    addr,
			c_comparr addr[],
			c_domint  pos,
			c_domenum moodd,
			c_rng     numrng,
			c_bltrng  numrange,
			c_bltmrng nummultirange
		)`, testTable)

	execQuery(t, ctx, sourceURL, schema)
	// the decoys shift every user-defined OID on the target
	execQuery(t, ctx, targetURL, `CREATE TYPE decoy_enum AS ENUM ('a');
		CREATE TYPE decoy_comp AS (x int);
		CREATE DOMAIN decoy_dom AS text;
		CREATE TYPE decoy_rng AS RANGE (subtype = int4);`)
	execQuery(t, ctx, targetURL, schema)

	require.NotEqual(t,
		readTypeOID(t, ctx, sourceURL, "mood"),
		readTypeOID(t, ctx, targetURL, "mood"),
		"decoy types did not shift the target OIDs, so this test proves nothing")

	execQuery(t, ctx, sourceURL, fmt.Sprintf(`INSERT INTO %s VALUES
		(1, 'happy', '{sad,ok}',  ROW('main st', 2), ARRAY[ROW('main st', 2)::addr], 5, 'ok', '[1,2]', '[1,2]', '{[1,2]}'),
		(2, 'sad',   '{}',        ROW(NULL, NULL),   '{}',                           0, NULL, 'empty', 'empty', '{}'),
		(3, NULL,    NULL,        NULL,              NULL,                           NULL, NULL, NULL, NULL, NULL)`,
		testTable))

	generator, err := NewSnapshotGenerator(ctx, &Config{
		URL: sourceURL,
		// no explicit format: this must exercise the binary default
		CopyPassthrough: &CopyPassthroughConfig{TargetURL: targetURL},
	}, &failingProcessor{t: t})
	require.NoError(t, err)
	defer generator.Close()

	require.NoError(t, generator.CreateSnapshot(ctx, &snapshot.Snapshot{
		SchemaTables: map[string][]string{"public": {testTable}},
	}))

	require.Equal(t,
		readRowsAsText(t, ctx, sourceURL, testTable),
		readRowsAsText(t, ctx, targetURL, testTable))
}

// no COPY carries all-generated rows
func Test_PostgresSnapshotGenerator_copyPassthrough_allGeneratedColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sourceURL, targetURL string
	sourceCleanup, err := testcontainers.SetupPostgresContainer(ctx, &sourceURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer sourceCleanup()
	targetCleanup, err := testcontainers.SetupPostgresContainer(ctx, &targetURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer targetCleanup()

	const testTable = "copy_passthrough_all_generated_test"
	schema := fmt.Sprintf("CREATE TABLE %s (only_generated int GENERATED ALWAYS AS (1) STORED)", testTable)
	execQuery(t, ctx, sourceURL, schema)
	execQuery(t, ctx, targetURL, schema)
	execQuery(t, ctx, sourceURL, fmt.Sprintf("INSERT INTO %s DEFAULT VALUES", testTable))
	execQuery(t, ctx, sourceURL, fmt.Sprintf("INSERT INTO %s DEFAULT VALUES", testTable))

	rows := &countingProcessor{}
	generator, err := NewSnapshotGenerator(ctx, &Config{
		URL:             sourceURL,
		CopyPassthrough: &CopyPassthroughConfig{TargetURL: targetURL},
	}, rows)
	require.NoError(t, err)
	defer generator.Close()

	require.NoError(t, generator.CreateSnapshot(ctx, &snapshot.Snapshot{
		SchemaTables: map[string][]string{"public": {testTable}},
	}))

	// events prove it decoded instead
	require.Equal(t, 2, rows.count())
}

type countingProcessor struct {
	events atomic.Int64
}

func (p *countingProcessor) ProcessWALEvent(context.Context, *wal.Event) error {
	p.events.Add(1)
	return nil
}

func (p *countingProcessor) Close() error { return nil }
func (p *countingProcessor) Name() string { return "countingProcessor" }
func (p *countingProcessor) count() int   { return int(p.events.Load()) }

func readTypeOID(t *testing.T, ctx context.Context, pgurl, typeName string) int {
	t.Helper()

	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var oid int
	require.NoError(t, conn.QueryRow(ctx, []any{&oid}, "SELECT $1::regtype::oid::int", typeName))
	return oid
}

type failingProcessor struct {
	t *testing.T
}

func (p *failingProcessor) ProcessWALEvent(_ context.Context, event *wal.Event) error {
	p.t.Errorf("copy passthrough emitted a wal event, so rows were decoded: %v", event)
	return nil
}

func (p *failingProcessor) Close() error { return nil }
func (p *failingProcessor) Name() string { return "failingProcessor" }

// compares rows across two instances
func readRowsAsText(t *testing.T, ctx context.Context, pgurl, tableName string) []string {
	t.Helper()

	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT (t.*)::text FROM %s t ORDER BY id", tableName))
	require.NoError(t, err)
	defer rows.Close()

	var out []string
	for rows.Next() {
		var row string
		require.NoError(t, rows.Scan(&row))
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, out, "no rows read from %s", tableName)
	return out
}
