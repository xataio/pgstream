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
	execQueryWithURL(t, ctx, snapshotPGURL, "create role test_role")

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
		if len(roles) != 1 {
			return false
		}
		require.Equal(t, "test_role", roles[0])

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
