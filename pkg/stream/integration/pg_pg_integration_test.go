// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

type informationSchemaColumn struct {
	name       string
	dataType   string
	isNullable string
}

type testTableColumn struct {
	id       int
	name     string
	username string
}

type idNameRow struct {
	id   int
	name string
}

func Test_PostgresToPostgres(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	testTable := "pg2pg_integration_test"

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string

		validation func() bool
	}{
		{
			name:  "create table",
			query: fmt.Sprintf("create table %s(id serial primary key, name text)", testTable),

			validation: func() bool {
				columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
				if len(columns) == 0 {
					return false
				}
				wantCols := []*informationSchemaColumn{
					{name: "id", dataType: "integer", isNullable: "NO"},
					{name: "name", dataType: "text", isNullable: "YES"},
				}
				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "alter table add column",
			query: fmt.Sprintf("alter table %s add column age int default 0", testTable),

			validation: func() bool {
				columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
				if len(columns) == 2 {
					return false
				}
				wantCols := []*informationSchemaColumn{
					{name: "id", dataType: "integer", isNullable: "NO"},
					{name: "name", dataType: "text", isNullable: "YES"},
					{name: "age", dataType: "integer", isNullable: "YES"},
				}

				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "alter table alter column type",
			query: fmt.Sprintf("alter table %s alter column age type bigint", testTable),

			validation: func() bool {
				columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
				wantCols := []*informationSchemaColumn{
					{name: "id", dataType: "integer", isNullable: "NO"},
					{name: "name", dataType: "text", isNullable: "YES"},
					{name: "age", dataType: "bigint", isNullable: "YES"},
				}

				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "alter table rename column",
			query: fmt.Sprintf("alter table %s rename column age to new_age", testTable),

			validation: func() bool {
				columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
				wantCols := []*informationSchemaColumn{
					{name: "id", dataType: "integer", isNullable: "NO"},
					{name: "name", dataType: "text", isNullable: "YES"},
					{name: "new_age", dataType: "bigint", isNullable: "YES"},
				}
				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "alter table drop column",
			query: fmt.Sprintf("alter table %s drop column new_age", testTable),

			validation: func() bool {
				columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
				if len(columns) == 3 {
					return false
				}
				wantCols := []*informationSchemaColumn{
					{name: "id", dataType: "integer", isNullable: "NO"},
					{name: "name", dataType: "text", isNullable: "YES"},
				}
				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "insert data",
			query: fmt.Sprintf("insert into %s(name) values('a')", testTable),

			validation: func() bool {
				columns := getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s", testTable))
				if len(columns) == 0 {
					return false
				}
				wantCols := []*testTableColumn{
					{id: 1, name: "a"},
				}
				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "update data",
			query: fmt.Sprintf("update %s set name='alice' where name='a'", testTable),

			validation: func() bool {
				columns := getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s", testTable))
				wantCols := []*testTableColumn{
					{id: 1, name: "alice"},
				}
				require.ElementsMatch(t, wantCols, columns)
				return true
			},
		},
		{
			name:  "delete data",
			query: fmt.Sprintf("delete from %s where name='alice'", testTable),

			validation: func() bool {
				columns := getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s where name='alice'", testTable))
				if len(columns) == 1 {
					return false
				}
				require.Empty(t, columns)
				return true
			},
		},
		{
			name:  "drop table",
			query: fmt.Sprintf("drop table %s", testTable),

			validation: func() bool {
				columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
				if len(columns) == 2 {
					return false
				}
				require.Empty(t, columns)
				return true
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execQuery(t, ctx, tc.query)

			timer := time.NewTimer(20 * time.Second)
			defer timer.Stop()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-timer.C:
					cancel()
					t.Error("timeout waiting for postgres sync")
					return
				case <-ticker.C:
					if tc.validation() {
						return
					}
				}
			}
		})
	}
}

func Test_PostgresToPostgres_Snapshot(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	// postgres container where pgstream hasn't been initialised to be used for
	// initial snapshot validation
	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTable := "pg2pg_snapshot_integration_test"
	// create table and populate it before initialising and running pgstream to
	// ensure the snapshot captures pre-existing schema and data properly
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("create table %s(id serial primary key, name text)", testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("insert into %s(name) values('a'),('b')", testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, "create role test_role;")
	// grant some privileges to the role to ensure these are captured in the snapshot
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("grant select on %s to test_role", testTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
		Processor: testPostgresProcessorCfg(),
	}
	initStream(t, ctx, snapshotPGURL)
	runStream(t, ctx, cfg)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)

	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	validation := func() bool {
		schemaColumns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		if len(schemaColumns) != 2 {
			return false
		}

		wantSchemaCols := []*informationSchemaColumn{
			{name: "id", dataType: "integer", isNullable: "NO"},
			{name: "name", dataType: "text", isNullable: "YES"},
		}
		require.ElementsMatch(t, wantSchemaCols, schemaColumns)

		columns := getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s", testTable))
		if len(columns) != 2 {
			return false
		}

		wantCols := []*testTableColumn{
			{id: 1, name: "a"},
			{id: 2, name: "b"},
		}
		require.ElementsMatch(t, wantCols, columns)

		roles := getRoles(t, ctx, targetConn)
		if len(roles) < 1 {
			return false
		}
		require.Contains(t, roles, "test_role")

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

func Test_PostgresToPostgres_IndexesAndConstraints(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	sourceConn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	suffix := time.Now().UnixNano()
	parentTable := fmt.Sprintf("pg2pg_parent_schema_%d", suffix)
	childTable := fmt.Sprintf("pg2pg_child_schema_%d", suffix)
	indexName := fmt.Sprintf("idx_parent_id_%d", suffix)
	indexRename := fmt.Sprintf("%s_renamed", indexName)
	checkConstraintName := fmt.Sprintf("chk_value_not_empty_%d", suffix)
	uniqueConstraintName := fmt.Sprintf("uq_value_%d", suffix)
	foreignKeyName := fmt.Sprintf("fk_child_parent_%d", suffix)

	defer execQuery(t, ctx, fmt.Sprintf("drop table if exists %s cascade", parentTable))
	defer execQuery(t, ctx, fmt.Sprintf("drop table if exists %s cascade", childTable))

	execQuery(t, ctx, fmt.Sprintf("create table %s (id serial primary key)", parentTable))

	createChildTable := fmt.Sprintf(`
		create table %s (
			id serial primary key,
			parent_id integer not null,
			value text not null unique,
			constraint %s check (value <> ''),
			constraint %s foreign key (parent_id) references %s(id) on delete cascade,
			constraint %s unique (value)
		)`, childTable, checkConstraintName, foreignKeyName, parentTable, uniqueConstraintName)
	execQuery(t, ctx, createChildTable)
	execQuery(t, ctx, fmt.Sprintf("create index %s on %s (parent_id)", indexName, childTable))

	sourceConstraints := getTableConstraints(t, ctx, sourceConn, "public", childTable)
	require.Contains(t, sourceConstraints, checkConstraintName)
	require.Contains(t, sourceConstraints, uniqueConstraintName)
	require.Contains(t, sourceConstraints, foreignKeyName)

	sourceIndexes := getTableIndexes(t, ctx, sourceConn, "public", childTable)
	require.Contains(t, sourceIndexes, indexName)

	require.Eventually(t, func() bool {
		targetConstraints := getTableConstraints(t, ctx, targetConn, "public", childTable)
		checkConstraint, ok := targetConstraints[checkConstraintName]
		if !ok {
			return false
		}
		if checkConstraint.definition != sourceConstraints[checkConstraintName].definition {
			return false
		}

		uniqueConstraint, ok := targetConstraints[uniqueConstraintName]
		if !ok {
			return false
		}
		if uniqueConstraint.definition != sourceConstraints[uniqueConstraintName].definition {
			return false
		}

		fkConstraint, ok := targetConstraints[foreignKeyName]
		if !ok {
			return false
		}
		return fkConstraint.definition == sourceConstraints[foreignKeyName].definition
	}, 20*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		targetIndexes := getTableIndexes(t, ctx, targetConn, "public", childTable)
		targetDefinition, ok := targetIndexes[indexName]
		if !ok {
			return false
		}
		return targetDefinition == sourceIndexes[indexName]
	}, 20*time.Second, 200*time.Millisecond)

	execQuery(t, ctx, fmt.Sprintf("alter index %s rename to %s", indexName, indexRename))
	sourceIndexes = getTableIndexes(t, ctx, sourceConn, "public", childTable)
	require.Contains(t, sourceIndexes, indexRename)
	require.NotContains(t, sourceIndexes, indexName)

	require.Eventually(t, func() bool {
		targetIndexes := getTableIndexes(t, ctx, targetConn, "public", childTable)
		if _, ok := targetIndexes[indexName]; ok {
			return false
		}
		targetDefinition, ok := targetIndexes[indexRename]
		if !ok {
			return false
		}
		return targetDefinition == sourceIndexes[indexRename]
	}, 20*time.Second, 200*time.Millisecond)
}

func Test_PostgresToPostgres_IdentityColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceConn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	runStream(t, ctx, cfg)

	testTable := "pg2pg_identity_test"

	// Create table with GENERATED ALWAYS AS IDENTITY column
	execQuery(t, ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name text
		)
	`, testTable))

	// Validate table schema is replicated
	require.Eventually(t, func() bool {
		columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		if len(columns) != 2 {
			return false
		}
		wantCols := []*informationSchemaColumn{
			{name: "id", dataType: "bigint", isNullable: "NO"},
			{name: "name", dataType: "text", isNullable: "YES"},
		}
		require.ElementsMatch(t, wantCols, columns)
		return true
	}, 20*time.Second, 200*time.Millisecond)

	// Insert data without specifying id - it should be generated by the identity sequence
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('Alice')", testTable))
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('Bob')", testTable))

	// Validate data is replicated with auto-generated IDs
	require.Eventually(t, func() bool {
		rows, err := targetConn.Query(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", testTable))
		require.NoError(t, err)
		defer rows.Close()

		type row struct {
			id   int64
			name string
		}

		results := []row{}
		for rows.Next() {
			var r row
			err := rows.Scan(&r.id, &r.name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		if len(results) != 2 {
			return false
		}

		// Verify IDs are auto-generated by the sequence (not manually set)
		require.Equal(t, int64(1), results[0].id)
		require.Equal(t, "Alice", results[0].name)
		require.Equal(t, int64(2), results[1].id)
		require.Equal(t, "Bob", results[1].name)

		return true
	}, 20*time.Second, 200*time.Millisecond)

	// Get the identity sequence name
	var sequenceName string
	err = sourceConn.QueryRow(ctx, []any{&sequenceName}, `
		SELECT pg_get_serial_sequence($1, 'id')
	`, testTable)
	require.NoError(t, err)
	require.NotEmpty(t, sequenceName)

	// Verify the backing sequence current value matches the number of inserted rows
	require.Eventually(t, func() bool {
		var sourceSeqValue, targetSeqValue int64
		err := sourceConn.QueryRow(ctx, []any{&sourceSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		err = targetConn.QueryRow(ctx, []any{&targetSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		// Both sequences should have the same last value after 2 inserts
		return sourceSeqValue == 2 && targetSeqValue == 2
	}, 20*time.Second, 200*time.Millisecond)

	// Insert one more row to verify sequence continues incrementing
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('Charlie')", testTable))

	require.Eventually(t, func() bool {
		rows, err := targetConn.Query(ctx, fmt.Sprintf("SELECT id, name FROM %s WHERE name='Charlie'", testTable))
		require.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var id int64
			var name string
			err := rows.Scan(&id, &name)
			require.NoError(t, err)

			// Verify the identity sequence incremented to 3
			require.Equal(t, int64(3), id)
			require.Equal(t, "Charlie", name)
			return true
		}
		return false
	}, 20*time.Second, 200*time.Millisecond)

	// Verify final sequence value is 3
	require.Eventually(t, func() bool {
		var sourceSeqValue, targetSeqValue int64
		err := sourceConn.QueryRow(ctx, []any{&sourceSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		err = targetConn.QueryRow(ctx, []any{&targetSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		return sourceSeqValue == 3 && targetSeqValue == 3
	}, 20*time.Second, 200*time.Millisecond)

	// Clean up
	execQuery(t, ctx, fmt.Sprintf("DROP TABLE %s", testTable))
}

// Test_PostgresToPostgres_AlwaysIdentityUpdate verifies that UPDATE events on a
// table with a GENERATED ALWAYS AS IDENTITY column are replicated successfully.
// Postgres rejects UPDATE ... SET <always-identity-col> = <value> with
// "column can only be updated to DEFAULT", so the column must be filtered out
// of the SET clause on the target.
func Test_PostgresToPostgres_AlwaysIdentityUpdate(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	runStream(t, ctx, cfg)

	testTable := "pg2pg_always_identity_update_test"
	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	// Table has a separate primary key plus a GENERATED ALWAYS AS IDENTITY
	// column. UPDATEs on `name` must not include `request_id` in the SET
	// clause, or Postgres rejects them.
	execQuery(t, ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id bigint PRIMARY KEY,
			request_id bigint GENERATED ALWAYS AS IDENTITY,
			name text
		)
	`, testTable))

	// Wait for the table schema to land on the target before querying it.
	require.Eventually(t, func() bool {
		columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		return len(columns) == 3
	}, 20*time.Second, 200*time.Millisecond, "table schema not replicated")

	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(id, name) VALUES(1, 'Alice')", testTable))
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(id, name) VALUES(2, 'Bob')", testTable))

	require.Eventually(t, func() bool {
		rows := getIDNameRows(t, ctx, targetConn,
			fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", testTable))
		return len(rows) == 2 && rows[0].name == "Alice" && rows[1].name == "Bob"
	}, 20*time.Second, 200*time.Millisecond, "initial inserts not replicated")

	// Capture the original request_id values on the target so we can verify
	// they are preserved across the UPDATE (since the SET clause must not
	// touch the always-identity column).
	var aliceReqIDBefore, bobReqIDBefore int64
	err = targetConn.QueryRow(ctx, []any{&aliceReqIDBefore},
		fmt.Sprintf("SELECT request_id FROM %s WHERE id = 1", testTable))
	require.NoError(t, err)
	err = targetConn.QueryRow(ctx, []any{&bobReqIDBefore},
		fmt.Sprintf("SELECT request_id FROM %s WHERE id = 2", testTable))
	require.NoError(t, err)

	// Perform UPDATEs that, pre-fix, produced
	// "column \"request_id\" can only be updated to DEFAULT" on the target.
	execQuery(t, ctx, fmt.Sprintf("UPDATE %s SET name = 'Alice2' WHERE id = 1", testTable))
	execQuery(t, ctx, fmt.Sprintf("UPDATE %s SET name = 'Bob2' WHERE id = 2", testTable))

	require.Eventually(t, func() bool {
		rows := getIDNameRows(t, ctx, targetConn,
			fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", testTable))
		return len(rows) == 2 && rows[0].name == "Alice2" && rows[1].name == "Bob2"
	}, 20*time.Second, 200*time.Millisecond, "UPDATE not replicated — likely rejected by always-identity rule")

	var aliceReqIDAfter, bobReqIDAfter int64
	err = targetConn.QueryRow(ctx, []any{&aliceReqIDAfter},
		fmt.Sprintf("SELECT request_id FROM %s WHERE id = 1", testTable))
	require.NoError(t, err)
	err = targetConn.QueryRow(ctx, []any{&bobReqIDAfter},
		fmt.Sprintf("SELECT request_id FROM %s WHERE id = 2", testTable))
	require.NoError(t, err)

	require.Equal(t, aliceReqIDBefore, aliceReqIDAfter, "request_id should be unchanged by UPDATE")
	require.Equal(t, bobReqIDBefore, bobReqIDAfter, "request_id should be unchanged by UPDATE")
}

func Test_PostgresToPostgres_Sequences(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	sourceConn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	suffix := time.Now().UnixNano()
	sequenceName := fmt.Sprintf("test_sequence_%d", suffix)
	sequenceRename := fmt.Sprintf("%s_renamed", sequenceName)
	testTable := fmt.Sprintf("test_sequence_table_%d", suffix)

	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", testTable))
	defer execQuery(t, ctx, fmt.Sprintf("DROP SEQUENCE IF EXISTS %s CASCADE", sequenceName))
	defer execQuery(t, ctx, fmt.Sprintf("DROP SEQUENCE IF EXISTS %s CASCADE", sequenceRename))

	// Create a sequence with specific parameters
	execQuery(t, ctx, fmt.Sprintf(`
		CREATE SEQUENCE %s
			AS bigint
			INCREMENT BY 5
			MINVALUE 10
			MAXVALUE 1000
			START WITH 50
			CYCLE
	`, sequenceName))

	sourceSequences := getSequences(t, ctx, sourceConn, "public")
	require.Contains(t, sourceSequences, sequenceName)

	sourceSeq := sourceSequences[sequenceName]
	require.Equal(t, "bigint", sourceSeq.dataType)
	require.Equal(t, "5", sourceSeq.increment)
	require.Equal(t, "10", sourceSeq.minimumValue)
	require.Equal(t, "1000", sourceSeq.maximumValue)
	require.Equal(t, "50", sourceSeq.startValue)
	require.Equal(t, "YES", sourceSeq.cycleOption)

	// Validate sequence is replicated to target
	require.Eventually(t, func() bool {
		targetSequences := getSequences(t, ctx, targetConn, "public")
		targetSeq, ok := targetSequences[sequenceName]
		if !ok {
			return false
		}
		return targetSeq.dataType == sourceSeq.dataType &&
			targetSeq.increment == sourceSeq.increment &&
			targetSeq.minimumValue == sourceSeq.minimumValue &&
			targetSeq.maximumValue == sourceSeq.maximumValue &&
			targetSeq.startValue == sourceSeq.startValue &&
			targetSeq.cycleOption == sourceSeq.cycleOption
	}, 20*time.Second, 200*time.Millisecond)

	// Create a table that uses the sequence
	execQuery(t, ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id bigint DEFAULT nextval('%s'),
			name text
		)
	`, testTable, sequenceName))

	// Validate table is replicated
	require.Eventually(t, func() bool {
		columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		return len(columns) == 2
	}, 20*time.Second, 200*time.Millisecond)

	// Insert rows to increment the sequence
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('row1')", testTable))
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('row2')", testTable))

	// Validate sequence increments after inserts (start=50, increment=5, so after 2 inserts: 50, 55)
	require.Eventually(t, func() bool {
		var sourceSeqValue, targetSeqValue int64
		err := sourceConn.QueryRow(ctx, []any{&sourceSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		err = targetConn.QueryRow(ctx, []any{&targetSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		// After 2 inserts with increment 5 starting from 50: should be at 55
		return sourceSeqValue == 55 && targetSeqValue == 55
	}, 20*time.Second, 200*time.Millisecond)

	// Validate the data was replicated with correct sequence values
	require.Eventually(t, func() bool {
		rows, err := targetConn.Query(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", testTable))
		require.NoError(t, err)
		defer rows.Close()

		type row struct {
			id   int64
			name string
		}

		results := []row{}
		for rows.Next() {
			var r row
			err := rows.Scan(&r.id, &r.name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		if len(results) != 2 {
			return false
		}

		// Verify IDs match the sequence values
		require.Equal(t, int64(50), results[0].id)
		require.Equal(t, "row1", results[0].name)
		require.Equal(t, int64(55), results[1].id)
		require.Equal(t, "row2", results[1].name)

		return true
	}, 20*time.Second, 200*time.Millisecond)

	// Alter sequence parameters
	execQuery(t, ctx, fmt.Sprintf(`
		ALTER SEQUENCE %s
			INCREMENT BY 10
			MAXVALUE 2000
			NO CYCLE
	`, sequenceName))

	sourceSequences = getSequences(t, ctx, sourceConn, "public")
	require.Contains(t, sourceSequences, sequenceName)

	sourceSeq = sourceSequences[sequenceName]
	require.Equal(t, "10", sourceSeq.increment)
	require.Equal(t, "2000", sourceSeq.maximumValue)
	require.Equal(t, "NO", sourceSeq.cycleOption)

	// Validate altered sequence in target
	require.Eventually(t, func() bool {
		targetSequences := getSequences(t, ctx, targetConn, "public")
		targetSeq, ok := targetSequences[sequenceName]
		if !ok {
			return false
		}
		return targetSeq.increment == "10" &&
			targetSeq.maximumValue == "2000" &&
			targetSeq.cycleOption == "NO"
	}, 20*time.Second, 200*time.Millisecond)

	// Insert another row to verify the altered increment (now 10 instead of 5)
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('row3')", testTable))

	// Validate sequence incremented by the new value (55 + 10 = 65)
	require.Eventually(t, func() bool {
		var sourceSeqValue, targetSeqValue int64
		err := sourceConn.QueryRow(ctx, []any{&sourceSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		err = targetConn.QueryRow(ctx, []any{&targetSeqValue}, fmt.Sprintf("SELECT last_value FROM %s", sequenceName))
		require.NoError(t, err)

		return sourceSeqValue == 65 && targetSeqValue == 65
	}, 20*time.Second, 200*time.Millisecond)

	// Drop table before testing sequence rename/drop operations
	execQuery(t, ctx, fmt.Sprintf("DROP TABLE %s CASCADE", testTable))

	// Wait for table to be dropped from target
	require.Eventually(t, func() bool {
		columns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		return len(columns) == 0
	}, 20*time.Second, 200*time.Millisecond)

	// Rename sequence
	execQuery(t, ctx, fmt.Sprintf("ALTER SEQUENCE %s RENAME TO %s", sequenceName, sequenceRename))

	sourceSequences = getSequences(t, ctx, sourceConn, "public")
	require.Contains(t, sourceSequences, sequenceRename)
	require.NotContains(t, sourceSequences, sequenceName)

	// Validate renamed sequence in target
	require.Eventually(t, func() bool {
		targetSequences := getSequences(t, ctx, targetConn, "public")
		if _, ok := targetSequences[sequenceName]; ok {
			return false
		}
		_, ok := targetSequences[sequenceRename]
		return ok
	}, 20*time.Second, 200*time.Millisecond)

	// Drop sequence
	execQuery(t, ctx, fmt.Sprintf("DROP SEQUENCE %s", sequenceRename))

	sourceSequences = getSequences(t, ctx, sourceConn, "public")
	require.NotContains(t, sourceSequences, sequenceRename)

	// Validate sequence is dropped from target
	require.Eventually(t, func() bool {
		targetSequences := getSequences(t, ctx, targetConn, "public")
		_, ok := targetSequences[sequenceRename]
		return !ok
	}, 20*time.Second, 200*time.Millisecond)
}

func getInformationSchemaColumns(t *testing.T, ctx context.Context, conn pglib.Querier, tableName string) []*informationSchemaColumn {
	rows, err := conn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", tableName)
	require.NoError(t, err)
	defer rows.Close()

	columns := []*informationSchemaColumn{}
	for rows.Next() {
		column := &informationSchemaColumn{}
		err := rows.Scan(&column.name, &column.dataType, &column.isNullable)
		require.NoError(t, err)
		columns = append(columns, column)
	}
	require.NoError(t, rows.Err())

	return columns
}

func getTestTableColumns(t *testing.T, ctx context.Context, conn pglib.Querier, query string, withGeneratedColumn ...bool) []*testTableColumn {
	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	columns := []*testTableColumn{}
	for rows.Next() {
		column := &testTableColumn{}
		if len(withGeneratedColumn) > 0 && withGeneratedColumn[0] {
			err = rows.Scan(&column.id, &column.name, &column.username)
		} else {
			err = rows.Scan(&column.id, &column.name)
		}
		require.NoError(t, err)
		columns = append(columns, column)
	}
	require.NoError(t, rows.Err())

	return columns
}

func getIDRows(t *testing.T, ctx context.Context, conn pglib.Querier, query string) []int {
	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

func getIDNameRows(t *testing.T, ctx context.Context, conn pglib.Querier, query string) []idNameRow {
	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var result []idNameRow
	for rows.Next() {
		var r idNameRow
		err := rows.Scan(&r.id, &r.name)
		require.NoError(t, err)
		result = append(result, r)
	}
	require.NoError(t, rows.Err())
	return result
}

func getRoles(t *testing.T, ctx context.Context, conn pglib.Querier) []string {
	rows, err := conn.Query(ctx, "select rolname from pg_roles where rolname not like 'pg_%' and rolname <> 'postgres'")
	require.NoError(t, err)
	defer rows.Close()

	roles := []string{}
	for rows.Next() {
		var role string
		err := rows.Scan(&role)
		require.NoError(t, err)
		roles = append(roles, role)
	}
	require.NoError(t, rows.Err())

	return roles
}

// Test_PostgresToPostgres_LargeIntegerPrecisionWAL is the WAL-replication
// counterpart of Test_SnapshotToPostgres_LargeIntegerPrecision. It pins down
// that INSERT / UPDATE events carrying large integers — both as plain bigint
// column values and as integer fields inside a jsonb payload — replicate
// bit-for-bit through the wal2json → listener → target path.
//
// Covers the WAL side of #824 (bigint) and #686 (jsonb large int).
func Test_PostgresToPostgres_LargeIntegerPrecisionWAL(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	testTable := "pg2pg_largeint_wal"

	// 9007199254740993 = 2^53 + 1, the smallest int64 that float64 cannot
	// represent exactly. 9223372036854775807 is MaxInt64.
	execQuery(t, ctx, fmt.Sprintf(
		`CREATE TABLE %s(
			id      bigint PRIMARY KEY,
			amount  bigint NOT NULL,
			payload jsonb  NOT NULL
		)`, testTable))

	type row struct {
		id      int64
		amount  int64
		payload string
	}

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)

	fetch := func() ([]row, error) {
		rows, err := targetConn.Query(ctx,
			fmt.Sprintf("SELECT id, amount, payload::text FROM %s ORDER BY id", testTable))
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

	waitFor := func(want []row) {
		t.Helper()
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-timer.C:
				cancel()
				t.Fatalf("timeout waiting for WAL replication; last fetch did not match %v", want)
			case <-ticker.C:
				got, err := fetch()
				if err != nil || len(got) != len(want) {
					continue
				}
				if reflect.DeepEqual(got, want) {
					require.Equal(t, want, got)
					return
				}
			}
		}
	}

	t.Run("insert bigint above 2^53 + jsonb with large int", func(t *testing.T) {
		execQuery(t, ctx, fmt.Sprintf(
			`INSERT INTO %s(id, amount, payload) VALUES
				(9007199254740993,    9007199254740993,    '{"n":9007199254740993}'),
				(9223372036854775807, 9223372036854775807, '{"n":9223372036854775807,"nested":{"k":1234567890123456789}}')`,
			testTable))

		waitFor([]row{
			{id: 9007199254740993, amount: 9007199254740993, payload: `{"n": 9007199254740993}`},
			{id: 9223372036854775807, amount: 9223372036854775807, payload: `{"n": 9223372036854775807, "nested": {"k": 1234567890123456789}}`},
		})
	})

	t.Run("update jsonb payload with new large int", func(t *testing.T) {
		execQuery(t, ctx, fmt.Sprintf(
			`UPDATE %s
			    SET payload = jsonb_set(payload, '{k2}', '987654321987654321'::jsonb)
			  WHERE id = 9223372036854775807`,
			testTable))

		// jsonb stores keys ordered by length then alphabetically, so
		// `n` (1) before `k2` (2) before `nested` (6).
		waitFor([]row{
			{id: 9007199254740993, amount: 9007199254740993, payload: `{"n": 9007199254740993}`},
			{id: 9223372036854775807, amount: 9223372036854775807, payload: `{"n": 9223372036854775807, "k2": 987654321987654321, "nested": {"k": 1234567890123456789}}`},
		})
	})

	t.Run("update bigint above 2^53 by +1", func(t *testing.T) {
		// The repro from #824: incrementing a bigint above 2^53 must
		// land on the destination as exactly source+1, not source+0.
		execQuery(t, ctx, fmt.Sprintf(
			`UPDATE %s SET amount = amount + 1 WHERE id = 9007199254740993`,
			testTable))

		waitFor([]row{
			{id: 9007199254740993, amount: 9007199254740994, payload: `{"n": 9007199254740993}`},
			{id: 9223372036854775807, amount: 9223372036854775807, payload: `{"n": 9223372036854775807, "k2": 987654321987654321, "nested": {"k": 1234567890123456789}}`},
		})
	})
}
