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
)

type informationSchemaColumn struct {
	name       string
	dataType   string
	isNullable string
}

type testTableColumn struct {
	id   int
	name string
}

func Test_PostgresToPostgres(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
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
				rows, err := targetConn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", testTable)
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
				rows, err := targetConn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", testTable)
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
				rows, err := targetConn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", testTable)
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
				rows, err := targetConn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", testTable)
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
				rows, err := targetConn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", testTable)
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
				rows, err := targetConn.Query(ctx, fmt.Sprintf("select id,name from %s", testTable))
				require.NoError(t, err)
				defer rows.Close()

				columns := []*testTableColumn{}
				for rows.Next() {
					column := &testTableColumn{}
					err := rows.Scan(&column.id, &column.name)
					require.NoError(t, err)
					columns = append(columns, column)
				}

				require.NoError(t, rows.Err())
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
				rows, err := targetConn.Query(ctx, fmt.Sprintf("select id,name from %s", testTable))
				require.NoError(t, err)
				defer rows.Close()

				columns := []*testTableColumn{}
				for rows.Next() {
					column := &testTableColumn{}
					err := rows.Scan(&column.id, &column.name)
					require.NoError(t, err)
					columns = append(columns, column)
				}

				require.NoError(t, rows.Err())
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
				rows, err := targetConn.Query(ctx, fmt.Sprintf("select id,name from %s where name='alice'", testTable))
				require.NoError(t, err)
				defer rows.Close()

				columns := []*testTableColumn{}
				for rows.Next() {
					column := &testTableColumn{}
					err := rows.Scan(&column.id, &column.name)
					require.NoError(t, err)
					columns = append(columns, column)
				}

				require.NoError(t, rows.Err())
				require.Empty(t, columns)

				return true
			},
		},
		{
			name:  "drop table",
			query: fmt.Sprintf("drop table %s", testTable),

			validation: func() bool {
				rows, err := targetConn.Query(ctx, "select column_name, data_type, is_nullable from information_schema.columns where table_name = $1", testTable)
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
				require.Empty(t, columns)

				return true
			},
		},
	}

	for _, tc := range tests {
		tc := tc
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
