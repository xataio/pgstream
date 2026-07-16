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
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

func Test_SnapshotToPostgres(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	// postgres container where pgstream hasn't been initialised to be used for
	// snapshot validation
	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(testTable string, opts ...option) {
		// create table and populate it before initialising and running pgstream to
		// ensure the snapshot captures pre-existing schema and data properly
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(`CREATE TABLE %s(id serial PRIMARY KEY, name TEXT, username TEXT GENERATED ALWAYS AS ('user_' || name ) STORED)`, testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("insert into %s(name) values('a'),('b')", testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("create role test_role_%s", testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("grant select on %s to test_role_%s", testTable, testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		validation := func() bool {
			schemaColumns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
			if len(schemaColumns) != 3 {
				return false
			}

			wantSchemaCols := []*informationSchemaColumn{
				{name: "id", dataType: "integer", isNullable: "NO"},
				{name: "name", dataType: "text", isNullable: "YES"},
				{name: "username", dataType: "text", isNullable: "YES"},
			}
			require.ElementsMatch(t, wantSchemaCols, schemaColumns)

			columns := getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name,username from %s", testTable), withGeneratedColumn)
			if len(columns) != 2 {
				return false
			}

			wantCols := []*testTableColumn{
				{id: 1, name: "a", username: "user_a"},
				{id: 2, name: "b", username: "user_b"},
			}
			require.ElementsMatch(t, wantCols, columns)

			roles := getRoles(t, ctx, targetConn)
			require.NotEmpty(t, roles)
			require.Contains(t, roles, "test_role_"+testTable)

			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for postgres snapshot sync")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("snapshot2pg_bulk_integration_test", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("snapshot2pg_batch_integration_test")
	})
}

// Test_SnapshotToPostgres_SelectedParentTableDoesNotCopyInheritedRows verifies
// that snapshotting a selected parent table does not copy rows stored in child
// tables through PostgreSQL inheritance.
func Test_SnapshotToPostgres_SelectedParentTableDoesNotCopyInheritedRows(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	parentTable := fmt.Sprintf("snapshot_parent_only_%d", suffix)
	childTable := fmt.Sprintf("snapshot_child_inherits_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`, parentTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s() INHERITS (%s)`, childTable, parentTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (1, 'parent-row')`, parentTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (2, 'child-row')`, childTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{parentTable}),
		Processor: testPostgresProcessorCfg(),
	}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	rows, err := targetConn.Query(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", parentTable))
	require.NoError(t, err)
	defer rows.Close()

	type parentRow struct {
		id   int
		name string
	}

	got := []parentRow{}
	for rows.Next() {
		var r parentRow
		require.NoError(t, rows.Scan(&r.id, &r.name))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []parentRow{{id: 1, name: "parent-row"}}, got)
}

// Test_SnapshotToPostgres_SchemaOnlyTables verifies that tables in the
// schema-only list get their DDL copied by the schema snapshot while their
// data is skipped, including for a schema that only appears in the
// schema-only list.
func Test_SnapshotToPostgres_SchemaOnlyTables(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	dataTable := fmt.Sprintf("snapshot_data_%d", suffix)
	schemaOnlyTable := fmt.Sprintf("snapshot_schema_only_%d", suffix)
	schemaOnlySchema := fmt.Sprintf("reports_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`, dataTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (1, 'a'),(2, 'b')`, dataTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`, schemaOnlyTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (1, 'not-copied')`, schemaOnlyTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE SCHEMA %s`, schemaOnlySchema))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s.audit_log(id INTEGER PRIMARY KEY, event TEXT)`, schemaOnlySchema))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s.audit_log(id, event) VALUES (1, 'not-copied')`, schemaOnlySchema))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{dataTable}),
		Processor: testPostgresProcessorCfg(),
	}
	cfg.Listener.Postgres.Snapshot.Adapter.SchemaOnlyTables = []string{schemaOnlyTable, schemaOnlySchema + ".*"}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// the data table has both its schema and its rows
	rows := getIDNameRows(t, ctx, targetConn, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", dataTable))
	require.Equal(t, []idNameRow{{id: 1, name: "a"}, {id: 2, name: "b"}}, rows)

	// the schema-only table exists with its full schema but no rows
	schemaColumns := getInformationSchemaColumns(t, ctx, targetConn, schemaOnlyTable)
	wantSchemaCols := []*informationSchemaColumn{
		{name: "id", dataType: "integer", isNullable: "NO"},
		{name: "name", dataType: "text", isNullable: "NO"},
	}
	require.ElementsMatch(t, wantSchemaCols, schemaColumns)

	var count int
	err = targetConn.QueryRow(ctx, []any{&count}, fmt.Sprintf("SELECT count(*) FROM %s", schemaOnlyTable))
	require.NoError(t, err)
	require.Zero(t, count, "schema-only table should have no rows on the target")

	// the schema-only schema exists with its table's DDL but no rows
	err = targetConn.QueryRow(ctx, []any{&count}, fmt.Sprintf("SELECT count(*) FROM %s.audit_log", schemaOnlySchema))
	require.NoError(t, err)
	require.Zero(t, count, "schema-only schema tables should have no rows on the target")
}

// Test_SnapshotToPostgres_CoalescedInheritedChildInsertReportsRowLoss is a
// synthetic regression for silent row loss during snapshot writes. The target
// has a stale conflicting row in the inherited child table. The batch writer
// coalesces the source child rows into one INSERT; with strict mode enabled
// the failed INSERT is reported to the caller instead of being dropped.
func Test_SnapshotToPostgres_CoalescedInheritedChildInsertReportsRowLoss(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sourceURL string
	sourceCleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &sourceURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer sourceCleanup()

	var targetURL string
	targetCleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &targetURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer targetCleanup()

	suffix := time.Now().UnixNano()
	parentTable := fmt.Sprintf("snapshot_parent_loss_%d", suffix)
	childTable := fmt.Sprintf("snapshot_child_loss_%d", suffix)
	childPK := fmt.Sprintf("%s_pkey", childTable)

	createSchema := func(url string) {
		execQueryWithURL(t, ctx, url, fmt.Sprintf(
			`CREATE TABLE %s(
				id integer NOT NULL,
				message text NOT NULL
			)`, parentTable))
		execQueryWithURL(t, ctx, url, fmt.Sprintf(
			`CREATE TABLE %s(
				child_note text NOT NULL
			) INHERITS (%s)`, childTable, parentTable))
		execQueryWithURL(t, ctx, url, fmt.Sprintf(
			`ALTER TABLE ONLY %s ADD CONSTRAINT %s PRIMARY KEY (id)`, childTable, childPK))
	}

	createSchema(sourceURL)
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(
		`INSERT INTO %s(id, message, child_note) VALUES
			(1, 'source-one', 'child-a'),
			(2, 'source-two', 'child-b')`, childTable))

	createSchema(targetURL)
	execQueryWithURL(t, ctx, targetURL, fmt.Sprintf(
		`INSERT INTO %s(id, message, child_note) VALUES
			(1, 'stale-target-row', 'stale-child')`, childTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(sourceURL, targetURL, []string{parentTable, childTable}),
		Processor: testPostgresProcessorCfgWithTargetURL(targetURL, withBatchSize(100), withStrictMode()),
	}
	err = stream.Snapshot(ctx, testLogger(), cfg, nil)

	type childRow struct {
		id        int
		message   string
		childNote string
	}
	fetchRows := func(url string) []childRow {
		conn, err := pglib.NewConn(ctx, url)
		require.NoError(t, err)
		defer conn.Close(ctx)

		rows, err := conn.Query(ctx, fmt.Sprintf(
			`SELECT id, message, child_note FROM ONLY %s ORDER BY id`, childTable))
		require.NoError(t, err)
		defer rows.Close()

		out := []childRow{}
		for rows.Next() {
			var r childRow
			require.NoError(t, rows.Scan(&r.id, &r.message, &r.childNote))
			out = append(out, r)
		}
		require.NoError(t, rows.Err())
		return out
	}

	sourceRows := fetchRows(sourceURL)
	targetRows := fetchRows(targetURL)
	require.ErrorContains(t, err, "strict mode: stopping on non-internal query failure",
		"snapshot should report dropped rows; source=%v target=%v", sourceRows, targetRows)
}

// Test_SnapshotToPostgres_IdentityOnlyTable verifies that tables where the only
// column is an identity column (e.g. id-only lookup tables) are correctly
// snapshotted, and that their values are preserved so that foreign key
// references from child tables remain valid.
func Test_SnapshotToPostgres_IdentityOnlyTable(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(suffix string, opts ...option) {
		parentTable := fmt.Sprintf("parent_identity_%s", suffix)
		childTable := fmt.Sprintf("child_identity_%s", suffix)

		// Create a parent table where id is the only column (identity).
		// This is the minimal repro: filterRowColumns used to filter out
		// all columns, producing zero insert queries.
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)`, parentTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id) VALUES (100),(200),(300)`, parentTable))

		// Create a child table that references the parent via FK.
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(
				id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				parent_id INTEGER NOT NULL REFERENCES %s(id),
				name TEXT
			)`, childTable, parentTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id, parent_id, name) VALUES (1, 100, 'a'),(2, 200, 'b'),(3, 300, 'c')`, childTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{parentTable, childTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		validation := func() bool {
			// Verify parent table: all 3 rows with exact id values preserved
			parentRows := getIDRows(t, ctx, targetConn, fmt.Sprintf("SELECT id FROM %s ORDER BY id", parentTable))
			if len(parentRows) != 3 {
				return false
			}
			require.Equal(t, []int{100, 200, 300}, parentRows)

			// Verify child table: all 3 rows with correct FK references
			childRows := getIDNameRows(t, ctx, targetConn, fmt.Sprintf("SELECT parent_id, name FROM %s ORDER BY id", childTable))
			if len(childRows) != 3 {
				return false
			}
			wantChildren := []idNameRow{
				{id: 100, name: "a"},
				{id: 200, name: "b"},
				{id: 300, name: "c"},
			}
			require.Equal(t, wantChildren, childRows)

			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for postgres snapshot sync of identity-only tables")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("batch")
	})
}

// Test_SnapshotToPostgres_CubeColumns verifies that snapshot/restore
// preserves cube column values end-to-end.
//
// Regression for #801: pgx's CopyFrom serialised cube values in binary
// format and the destination COPY rejected the rows with "cube dimension
// is too large" because the leading bytes of the text representation
// were interpreted as the binary header.
func Test_SnapshotToPostgres_CubeColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(suffix string, opts ...option) {
		testTable := fmt.Sprintf("cube_columns_%s", suffix)

		// install cube on both source and target; the test row uses a
		// 5-D cube and a 3-D box to exercise the binary-header failure
		// mode.
		for _, url := range []string{snapshotPGURL, targetPGURL} {
			execQueryWithURL(t, ctx, url, "CREATE EXTENSION IF NOT EXISTS cube")
		}

		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(
				id  INTEGER PRIMARY KEY,
				vec cube NOT NULL
			)`, testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id, vec) VALUES
				(1, cube(ARRAY[1.0, 2.0, 3.0, 4.0, 5.0])),
				(2, cube(ARRAY[0.5])),
				(3, cube('(10, 20, 30),(40, 50, 60)'))`,
			testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)
		sourceConn, err := pglib.NewConn(ctx, snapshotPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		type row struct {
			id  int
			vec string
		}
		// Pull the canonical text form from the source so the assertion
		// doesn't bake in cube output formatting quirks.
		query := fmt.Sprintf("SELECT id, vec::text FROM %s ORDER BY id", testTable)
		fetch := func(conn pglib.Querier) ([]row, error) {
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			out := []row{}
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.vec); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			return out, rows.Err()
		}

		want, err := fetch(sourceConn)
		require.NoError(t, err)
		require.Len(t, want, 3)

		validation := func() bool {
			got, err := fetch(targetConn)
			if err != nil || len(got) != len(want) {
				return false
			}
			require.Equal(t, want, got)
			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for postgres snapshot sync of cube columns")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("batch")
	})
}

// Test_SnapshotToPostgres_LtreeColumns verifies that snapshot/restore
// preserves ltree column values end-to-end. ltree has the same root cause
// as cube (#801): pgx has no binary codec for it, so binary COPY misreads
// the leading byte of the text rep as the binary ltree version number and
// errors with "unsupported ltree version number N".
func Test_SnapshotToPostgres_LtreeColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(suffix string, opts ...option) {
		testTable := fmt.Sprintf("ltree_columns_%s", suffix)
		for _, url := range []string{snapshotPGURL, targetPGURL} {
			execQueryWithURL(t, ctx, url, "CREATE EXTENSION IF NOT EXISTS ltree")
		}

		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(
				id   INTEGER PRIMARY KEY,
				path ltree NOT NULL
			)`, testTable))
		// ltree labels are [A-Za-z0-9_]+ on PG14 (the test container);
		// hyphens are PG16+ so they're avoided here.
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id, path) VALUES
				(1, 'a.b.c'),
				(2, 'root.x1.y2.z3.leaf'),
				(3, 'Root_1.Sub_2.leaf_3_x')`,
			testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)
		sourceConn, err := pglib.NewConn(ctx, snapshotPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		type row struct {
			id   int
			path string
		}
		query := fmt.Sprintf("SELECT id, path::text FROM %s ORDER BY id", testTable)
		fetch := func(conn pglib.Querier) ([]row, error) {
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			out := []row{}
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.path); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			return out, rows.Err()
		}

		want, err := fetch(sourceConn)
		require.NoError(t, err)
		require.Len(t, want, 3)

		validation := func() bool {
			got, err := fetch(targetConn)
			if err != nil || len(got) != len(want) {
				return false
			}
			require.Equal(t, want, got)
			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for postgres snapshot sync of ltree columns")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("batch")
	})
}

// Test_SnapshotToPostgres_ClusteredIndex verifies that ALTER TABLE ... CLUSTER
// ON ... is restored after its referenced index exists.
func Test_SnapshotToPostgres_ClusteredIndex(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	testTable := fmt.Sprintf("clustered_snapshot_%d", suffix)
	indexName := fmt.Sprintf("clustered_snapshot_idx_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(
			id integer PRIMARY KEY,
			created_at timestamptz NOT NULL
		)`, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, created_at) VALUES
			(1, '2026-01-01T00:00:00Z'),
			(2, '2026-01-02T00:00:00Z')`, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE INDEX %s ON %s(created_at)`, indexName, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`ALTER TABLE %s CLUSTER ON %s`, testTable, indexName))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{"public.*"}),
		Processor: testPostgresProcessorCfg(),
	}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	var clustered bool
	err = targetConn.QueryRow(ctx, []any{&clustered}, `
		SELECT i.indisclustered
		FROM pg_index i
		JOIN pg_class idx ON idx.oid = i.indexrelid
		JOIN pg_class tbl ON tbl.oid = i.indrelid
		JOIN pg_namespace nsp ON nsp.oid = tbl.relnamespace
		WHERE nsp.nspname = 'public'
		  AND tbl.relname = $1
		  AND idx.relname = $2
	`, testTable, indexName)
	require.NoError(t, err)
	require.True(t, clustered)
}

// Test_SnapshotToPostgres_MaterializedViewRefresh verifies that materialized
// views restored from a schema-only dump are refreshed after pgstream copies
// the base table data.
func Test_SnapshotToPostgres_MaterializedViewRefresh(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	baseTable := fmt.Sprintf("mv_base_%d", suffix)
	viewName := fmt.Sprintf("mv_summary_%d", suffix)
	indexName := fmt.Sprintf("mv_summary_idx_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(
			id integer PRIMARY KEY,
			name text NOT NULL
		)`, baseTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES
			(1, 'alpha'),
			(2, 'beta')`, baseTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE MATERIALIZED VIEW %s AS
			SELECT id, upper(name) AS display_name
			FROM %s
			WITH NO DATA`, viewName, baseTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE UNIQUE INDEX %s ON %s(id)`, indexName, viewName))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{"public.*"}),
		Processor: testPostgresProcessorCfg(),
	}
	cfg.Listener.Postgres.Snapshot.Schema.DumpRestore.RefreshMaterializedViews = true
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	var count int
	err = targetConn.QueryRow(ctx, []any{&count}, fmt.Sprintf("SELECT count(*) FROM %s", viewName))
	require.NoError(t, err)
	require.Equal(t, 2, count)

	indexes := getTableIndexes(t, ctx, targetConn, "public", viewName)
	require.Contains(t, indexes, indexName)
}

// Test_SnapshotToPostgres_SkipsLegacyPLPGSQLHandlers verifies that legacy
// public PL/pgSQL handler functions from a source dump are not restored as
// ordinary user functions, while normal user-defined functions are preserved.
func Test_SnapshotToPostgres_SkipsLegacyPLPGSQLHandlers(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	testTable := fmt.Sprintf("legacy_plpgsql_snapshot_%d", suffix)
	regularFunction := fmt.Sprintf("snapshot_regular_fn_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, `
		CREATE FUNCTION public.plpgsql_call_handler() RETURNS language_handler
		AS '$libdir/plpgsql', 'plpgsql_call_handler'
		LANGUAGE C`)
	execQueryWithURL(t, ctx, snapshotPGURL, `
		CREATE FUNCTION public.plpgsql_validator(oid) RETURNS void
		AS '$libdir/plpgsql', 'plpgsql_validator'
		LANGUAGE C`)
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE FUNCTION public.%s() RETURNS integer
		LANGUAGE plpgsql
		AS $$ BEGIN RETURN 42; END $$`, regularFunction))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(id integer PRIMARY KEY)`, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id) VALUES (1)`, testTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{"public.*"}),
		Processor: testPostgresProcessorCfg(),
	}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	var legacyHandlerCount int
	err = targetConn.QueryRow(ctx, []any{&legacyHandlerCount}, `
		SELECT count(*)
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
		  AND p.proname IN ('plpgsql_call_handler', 'plpgsql_validator')
	`)
	require.NoError(t, err)
	require.Zero(t, legacyHandlerCount)

	var got int
	err = targetConn.QueryRow(ctx, []any{&got}, fmt.Sprintf("SELECT public.%s()", regularFunction))
	require.NoError(t, err)
	require.Equal(t, 42, got)
}

// Test_SnapshotToPostgres_SQLStandardFunctionBodies verifies that functions
// with SQL-standard bodies (LANGUAGE sql BEGIN ATOMIC / RETURN), whose bodies
// are parsed and validated at creation time, are restored after indices and
// constraints. A body such as `SELECT c.id, c.name ... GROUP BY c.id` is only
// valid while the primary key on c exists (functional dependency); pgstream
// defers primary key creation until after the data load, so creating the
// function in the initial schema phase fails with `column "c.name" must
// appear in the GROUP BY clause or be used in an aggregate function`.
func Test_SnapshotToPostgres_SQLStandardFunctionBodies(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	testTable := fmt.Sprintf("atomic_fn_customers_%d", suffix)
	atomicFunction := fmt.Sprintf("atomic_fn_names_%d", suffix)
	returnFunction := fmt.Sprintf("return_fn_count_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(id integer PRIMARY KEY, name text NOT NULL)`, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (1, 'a'),(2, 'b')`, testTable))
	// both function bodies rely on the primary key of the table for the GROUP
	// BY functional dependency and are validated at creation time
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE FUNCTION %s() RETURNS TABLE(id integer, name text)
		LANGUAGE sql
		BEGIN ATOMIC
			SELECT c.id, c.name FROM %s c GROUP BY c.id;
		END`, atomicFunction, testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE FUNCTION %s() RETURNS bigint
		LANGUAGE sql
		RETURN (SELECT count(*) FROM (SELECT c.id, c.name FROM %s c GROUP BY c.id) sub)`,
		returnFunction, testTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{"public.*"}),
		Processor: testPostgresProcessorCfg(),
	}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	rows := getIDNameRows(t, ctx, targetConn, fmt.Sprintf("SELECT id, name FROM %s() ORDER BY id", atomicFunction))
	require.Equal(t, []idNameRow{{id: 1, name: "a"}, {id: 2, name: "b"}}, rows)

	var count int64
	err = targetConn.QueryRow(ctx, []any{&count}, fmt.Sprintf("SELECT %s()", returnFunction))
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

// Test_SnapshotToPostgres_CircularDependencyView verifies that views involved
// in a circular dependency (view -> function -> view) are restored correctly.
// pg_dump breaks the cycle by emitting a dummy `CREATE VIEW ... SELECT
// NULL::...` (no FROM clause) upfront and a `CREATE OR REPLACE VIEW` with the
// real definition at the end of the dump. The real definition is validated at
// creation time: when it relies on the base table's primary key for the GROUP
// BY functional dependency, it must be restored after indices and constraints.
// The dummy view, on the other hand, must be restored early so that functions
// referencing the view's row type can be created.
func Test_SnapshotToPostgres_CircularDependencyView(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := time.Now().UnixNano()
	baseTable := fmt.Sprintf("circ_base_%d", suffix)
	viewName := fmt.Sprintf("circ_view_%d", suffix)
	fnName := fmt.Sprintf("circ_fn_%d", suffix)

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE TABLE %s(id integer PRIMARY KEY, name text NOT NULL)`, baseTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`INSERT INTO %s(id, name) VALUES (1, 'a'),(2, 'b')`, baseTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE VIEW %s AS SELECT c.id, c.name FROM %s c GROUP BY c.id`, viewName, baseTable))
	// the function depends on the view through its return type only, so
	// querying it doesn't recurse
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE FUNCTION %s() RETURNS SETOF %s LANGUAGE sql STABLE AS $$ SELECT id, name FROM %s $$`,
		fnName, viewName, baseTable))
	// make the view depend on the function to close the cycle; the GROUP BY
	// relies on the base table's primary key (functional dependency)
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		`CREATE OR REPLACE VIEW %s AS
			SELECT c.id, c.name FROM %s c
			WHERE c.id IN (SELECT id FROM %s())
			GROUP BY c.id`, viewName, baseTable, fnName))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{"public.*"}),
		Processor: testPostgresProcessorCfg(),
	}
	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// the view must hold the real definition, not pg_dump's dummy NULL
	// placeholder, and must return the snapshotted base table rows
	var viewDef string
	err = targetConn.QueryRow(ctx, []any{&viewDef},
		fmt.Sprintf("SELECT pg_get_viewdef('public.%s'::regclass)", viewName))
	require.NoError(t, err)
	require.Contains(t, viewDef, "GROUP BY")

	rows := getIDNameRows(t, ctx, targetConn, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", viewName))
	require.Equal(t, []idNameRow{{id: 1, name: "a"}, {id: 2, name: "b"}}, rows)
}

// Test_SnapshotToPostgres_IdentityAndGeneratedColumns verifies that identity
// columns have their values preserved while generated (stored) columns are
// correctly excluded from inserts and recomputed by the target database.
func Test_SnapshotToPostgres_IdentityAndGeneratedColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(suffix string, opts ...option) {
		testTable := fmt.Sprintf("identity_generated_%s", suffix)

		// Table with both an identity column (id) and a generated stored column (display_name).
		// The identity value must be preserved; the generated column must be excluded
		// from inserts and recomputed on the target.
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(
				id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				name TEXT NOT NULL,
				display_name TEXT GENERATED ALWAYS AS ('item_' || name) STORED
			)`, testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id, name) VALUES (10, 'alpha'),(20, 'beta'),(30, 'gamma')`, testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		validation := func() bool {
			rows := getTestTableColumns(t, ctx, targetConn,
				fmt.Sprintf("select id, name, display_name from %s order by id", testTable), withGeneratedColumn)
			if len(rows) != 3 {
				return false
			}

			want := []*testTableColumn{
				{id: 10, name: "alpha", username: "item_alpha"},
				{id: 20, name: "beta", username: "item_beta"},
				{id: 30, name: "gamma", username: "item_gamma"},
			}
			require.Equal(t, want, rows)
			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for postgres snapshot sync of identity+generated table")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("batch")
	})
}

// Test_SnapshotToPostgres_JSONNullFidelity verifies that snapshot/restore
// preserves the distinction between SQL NULL and the JSON null value
// ('null'::jsonb) in json/jsonb columns.
//
// pgx's default json/jsonb codec unmarshals values into Go `any`, which turns
// JSON null into Go nil — indistinguishable from SQL NULL. Without raw JSON
// decoding on the snapshot reader, the writer emits SQL NULL instead: nullable
// json/jsonb columns are silently corrupted, and columns declared
// `jsonb NOT NULL DEFAULT 'null'::jsonb` (a pattern seen in the wild) make the
// whole table snapshot fail with a not-null constraint violation on insert.
func Test_SnapshotToPostgres_JSONNullFidelity(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(suffix string, opts ...option) {
		testTable := fmt.Sprintf("json_null_%s", suffix)

		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(
				id       bigint PRIMARY KEY,
				required jsonb  NOT NULL DEFAULT 'null'::jsonb,
				optional jsonb,
				doc      json
			)`, testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id, required, optional, doc) VALUES
				(1, 'null'::jsonb, NULL, 'null'::json),
				(2, '{"a": 1}'::jsonb, 'null'::jsonb, '{"b": [null]}'::json)`,
			testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)
		sourceConn, err := pglib.NewConn(ctx, snapshotPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		// text projections keep the comparison independent of any client-side
		// json decoding; *string distinguishes SQL NULL (nil pointer) from the
		// JSON null value (the string "null").
		type row struct {
			id       int64
			required string
			optional *string
			doc      *string
		}
		query := fmt.Sprintf(
			"SELECT id, required::text, optional::text, doc::text FROM %s ORDER BY id", testTable)
		fetch := func(conn pglib.Querier) ([]row, error) {
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			out := []row{}
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.required, &r.optional, &r.doc); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			return out, rows.Err()
		}

		want, err := fetch(sourceConn)
		require.NoError(t, err)
		require.Len(t, want, 2)
		// Belt-and-braces: confirm the source stores the JSON null value (the
		// text "null"), not SQL NULL. If this fails the test setup is wrong,
		// not the production code.
		require.Equal(t, "null", want[0].required)
		require.Nil(t, want[0].optional)
		require.NotNil(t, want[1].optional)
		require.Equal(t, "null", *want[1].optional)

		validation := func() bool {
			got, err := fetch(targetConn)
			if err != nil || len(got) != len(want) {
				return false
			}
			require.Equal(t, want, got)
			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for json-null snapshot sync")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("batch")
	})
}

// Test_SnapshotToPostgres_LargeIntegerPrecision verifies that snapshot/restore
// preserves int64 precision for both:
//   - plain bigint columns whose value exceeds 2^53 (#824), and
//   - large integer values embedded inside json/jsonb columns (#686).
//
// Without the custom internal/json.UnmarshalUseInt64 decoder, JSON integers
// are silently rounded to float64 — the destination row diverges from the
// source by one or more units with no error or warning.
func Test_SnapshotToPostgres_LargeIntegerPrecision(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(suffix string, opts ...option) {
		testTable := fmt.Sprintf("largeint_%s", suffix)

		// 9007199254740993 = 2^53 + 1, the smallest int64 that float64
		// cannot represent exactly. 9223372036854775807 is MaxInt64.
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`CREATE TABLE %s(
				id      bigint PRIMARY KEY,
				amount  bigint NOT NULL,
				payload jsonb  NOT NULL
			)`, testTable))
		execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
			`INSERT INTO %s(id, amount, payload) VALUES
				(9007199254740993,    9007199254740993,    '{"n":9007199254740993}'),
				(9223372036854775807, 9223372036854775807, '{"n":9223372036854775807,"nested":{"k":1234567890123456789}}')`,
			testTable))

		cfg := &stream.Config{
			Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
			Processor: testPostgresProcessorCfg(opts...),
		}
		initStream(t, ctx, snapshotPGURL)
		runSnapshot(t, ctx, cfg)

		targetConn, err := pglib.NewConn(ctx, targetPGURL)
		require.NoError(t, err)
		sourceConn, err := pglib.NewConn(ctx, snapshotPGURL)
		require.NoError(t, err)

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		type row struct {
			id      int64
			amount  int64
			payload string
		}
		query := fmt.Sprintf(
			"SELECT id, amount, payload::text FROM %s ORDER BY id", testTable)
		fetch := func(conn pglib.Querier) ([]row, error) {
			rows, err := conn.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			out := []row{}
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.amount, &r.payload); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			return out, rows.Err()
		}

		want, err := fetch(sourceConn)
		require.NoError(t, err)
		require.Len(t, want, 2)
		// Belt-and-braces: confirm the source actually stores the
		// large ints we asked for. If this fails the test setup is
		// wrong, not the production code.
		require.Equal(t, int64(9007199254740993), want[0].id)
		require.Equal(t, int64(9223372036854775807), want[1].id)
		require.Contains(t, want[1].payload, `"k": 1234567890123456789`)

		validation := func() bool {
			got, err := fetch(targetConn)
			if err != nil || len(got) != len(want) {
				return false
			}
			require.Equal(t, want, got)
			return true
		}

		for {
			select {
			case <-timer.C:
				cancel()
				t.Error("timeout waiting for large-int snapshot sync")
				return
			case <-ticker.C:
				if validation() {
					return
				}
			}
		}
	}

	t.Run("bulk ingest", func(t *testing.T) {
		run("bulk", withBulkIngestionEnabled())
	})
	t.Run("batch writer", func(t *testing.T) {
		run("batch")
	})
}
