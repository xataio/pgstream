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

func Test_PostgresToPostgres(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
		Processor: testPostgresProcessorCfg(pgurl, withoutBulkIngestion),
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
		Processor: testPostgresProcessorCfg(snapshotPGURL, withoutBulkIngestion),
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

func Test_PostgresToPostgres_SchemaObjects(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
		Processor: testPostgresProcessorCfg(pgurl, withoutBulkIngestion),
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

func Test_PostgresToPostgres_Sequences(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
		Processor: testPostgresProcessorCfg(pgurl, withoutBulkIngestion),
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

	defer execQuery(t, ctx, fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", sequenceName))
	defer execQuery(t, ctx, fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", sequenceRename))

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
