// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
)

func TestDDLAdapter_walDataToQueries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	id := xid.New()

	table1 := "table1"
	table2 := "table2"

	testLogEntry := func(version int64) *schemalog.LogEntry {
		return &schemalog.LogEntry{
			ID:         id,
			Version:    version,
			SchemaName: testSchema,
			CreatedAt:  schemalog.NewSchemaCreatedAtTimestamp(now),
		}
	}

	tests := []struct {
		name         string
		querier      schemalogQuerier
		schemaDiffer schemaDiffer
		logEntry     *schemalog.LogEntry

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "ok - initial schema",
			querier: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error) {
					return nil, errors.New("unexpected call to FetchFn")
				},
			},
			schemaDiffer: func(old, new *schemalog.LogEntry) *schemalog.Diff {
				return &schemalog.Diff{
					TablesChanged: []schemalog.TableDiff{
						{
							TableName: table2,
							TableNameChange: &schemalog.ValueChange[string]{
								Old: table1,
								New: table2,
							},
						},
					},
				}
			},
			logEntry: testLogEntry(0),

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf(createSchemaIfNotExistsQuery, pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table2,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedTableName(testSchema, table1), table2),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - fetching existing schema",
			querier: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchema, schemaName)
					require.Equal(t, int(0), version)
					return testLogEntry(0), nil
				},
			},
			schemaDiffer: func(old, new *schemalog.LogEntry) *schemalog.Diff {
				return &schemalog.Diff{
					TablesChanged: []schemalog.TableDiff{
						{
							TableName: table2,
							TableNameChange: &schemalog.ValueChange[string]{
								Old: table1,
								New: table2,
							},
						},
					},
				}
			},
			logEntry: testLogEntry(1),

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf(createSchemaIfNotExistsQuery, pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table2,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedTableName(testSchema, table1), table2),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - fetching schema log",
			querier: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},
			logEntry: testLogEntry(1),

			wantQueries: nil,
			wantErr:     errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := ddlAdapter{
				schemalogQuerier: tc.querier,
				schemaDiffer:     tc.schemaDiffer,
			}

			queries, err := a.schemaLogToQueries(context.Background(), tc.logEntry)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func TestDDLAdapter_schemaDiffToQueries(t *testing.T) {
	t.Parallel()

	table1 := "test-table-1"
	table2 := "test-table-2"
	defaultAge := "0"
	defaultIdentity := "d"
	alwaysIdentity := "a"

	tests := []struct {
		name string
		diff *schemalog.Diff

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "ok - empty schema diff",
			diff: &schemalog.Diff{},

			wantQueries: []*query{},
			wantErr:     nil,
		},
		{
			name: "ok - schema dropped",
			diff: &schemalog.Diff{
				SchemaDropped: true,
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table removed",
			diff: &schemalog.Diff{
				TablesRemoved: []schemalog.Table{
					{Name: table1},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name:              table1,
						PrimaryKeyColumns: []string{"id"},
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: true},
							{Name: "name", DataType: "text", Nullable: true, Unique: true},
							{Name: "age", DataType: "int", Nullable: false, DefaultValue: &defaultAge},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"name\" text,\n\"age\" int NOT NULL DEFAULT 0,\nUNIQUE (\"name\"),\nPRIMARY KEY (\"id\")\n)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added no primary key or unique constraint",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name: table1,
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: false},
							{Name: "name", DataType: "text", Nullable: true, Unique: false},
							{Name: "age", DataType: "int", Nullable: false, DefaultValue: &defaultAge},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"name\" text,\n\"age\" int NOT NULL DEFAULT 0)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added with indexes",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name: table1,
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: false},
							{Name: "name", DataType: "text", Nullable: true, Unique: false},
						},
						Indexes: []schemalog.Index{
							{
								Name:       "idx_name",
								Definition: fmt.Sprintf("CREATE INDEX idx_name ON %s (\"name\")", quotedTableName(testSchema, table1)),
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"name\" text)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_name ON %s (\"name\")", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added with unique constraint backed index",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name: table1,
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false},
							{Name: "value", DataType: "text", Nullable: false},
						},
						Indexes: []schemalog.Index{
							{
								Name:       "uq_value",
								Definition: fmt.Sprintf("CREATE UNIQUE INDEX uq_value ON %s (\"value\")", quotedTableName(testSchema, table1)),
							},
						},
						Constraints: []schemalog.Constraint{
							{Name: "uq_value", Type: "UNIQUE", Definition: "UNIQUE (\"value\")"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"value\" text NOT NULL)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (\"value\")", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("uq_value")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added with constraints and foreign keys",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name: table1,
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: false},
							{Name: "ref_id", DataType: "uuid", Nullable: false, Unique: false},
						},
						Constraints: []schemalog.Constraint{
							{Name: "check_id_not_null", Type: "CHECK", Definition: "CHECK ((\"id\" IS NOT NULL))"},
						},
						ForeignKeys: []schemalog.ForeignKey{
							{Name: "fk_ref", Definition: "FOREIGN KEY (\"ref_id\") REFERENCES other(id)"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"ref_id\" uuid NOT NULL)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK ((\"id\" IS NOT NULL))", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("check_id_not_null")),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (\"ref_id\") REFERENCES other(id)", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("fk_ref")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed adds unique constraint without duplicate index",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						IndexesAdded: []schemalog.Index{
							{
								Name:       "uq_value",
								Definition: fmt.Sprintf("CREATE UNIQUE INDEX uq_value ON %s (\"value\")", quotedTableName(testSchema, table1)),
							},
						},
						ConstraintsAdded: []schemalog.Constraint{
							{Name: "uq_value", Type: "UNIQUE", Definition: "UNIQUE (\"value\")"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (\"value\")", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("uq_value")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table renamed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table2,
						TableNameChange: &schemalog.ValueChange[string]{
							Old: table1,
							New: table2,
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table2,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedTableName(testSchema, table1), table2),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column dropped",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsRemoved: []schemalog.Column{
							{Name: "age"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s DROP COLUMN \"age\"", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column added",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsAdded: []schemalog.Column{
							{Name: "age", DataType: "int", Nullable: false, DefaultValue: &defaultAge},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"age\" int NOT NULL DEFAULT 0", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column renamed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								NameChange: &schemalog.ValueChange[string]{Old: "name", New: "new_name"},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME COLUMN \"name\" TO \"new_name\"", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column type changed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								TypeChange: &schemalog.ValueChange[string]{Old: "uuid", New: "int"},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"name\" TYPE int", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column changed to nullable",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								NullChange: &schemalog.ValueChange[bool]{Old: false, New: true},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"name\" DROP NOT NULL", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column changed to not nullable",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								NullChange: &schemalog.ValueChange[bool]{Old: true, New: false},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"name\" SET NOT NULL", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column default removed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:    "age",
								DefaultChange: &schemalog.ValueChange[*string]{Old: &defaultAge, New: nil},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"age\" DROP DEFAULT", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column default changed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:    "age",
								DefaultChange: &schemalog.ValueChange[*string]{Old: nil, New: &defaultAge},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"age\" SET DEFAULT 0", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column identity changed from always to default",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:     "id",
								IdentityChange: &schemalog.ValueChange[string]{Old: alwaysIdentity, New: defaultIdentity},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"id\" SET GENERATED BY DEFAULT", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column identity changed from default to always",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:     "id",
								IdentityChange: &schemalog.ValueChange[string]{Old: defaultIdentity, New: alwaysIdentity},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"id\" SET GENERATED ALWAYS", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column identity added",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:     "id",
								IdentityChange: &schemalog.ValueChange[string]{Old: "", New: alwaysIdentity},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"id\" ADD GENERATED ALWAYS AS IDENTITY", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column identity dropped",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:     "id",
								IdentityChange: &schemalog.ValueChange[string]{Old: alwaysIdentity, New: ""},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"id\" DROP IDENTITY", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column generated dropped",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:      "id",
								GeneratedChange: &schemalog.ValueChange[bool]{Old: true, New: false},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"id\" DROP EXPRESSION", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, indexes updated",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						IndexesChanged: []string{
							fmt.Sprintf("ALTER INDEX %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, "idx_old"), pglib.QuoteIdentifier("idx_new")),
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER INDEX %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, "idx_old"), pglib.QuoteIdentifier("idx_new")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, constraints updated",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ConstraintsRemoved: []schemalog.Constraint{
							{Name: "check_old", Definition: "CHECK ((\"id\" > 0))"},
						},
						ConstraintsAdded: []schemalog.Constraint{
							{Name: "check_new", Definition: "CHECK ((\"id\" <> 0))"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("check_old")),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK ((\"id\" <> 0))", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("check_new")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, foreign keys updated",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ForeignKeysRemoved: []schemalog.ForeignKey{
							{Name: "fk_old", Definition: "FOREIGN KEY (\"id\") REFERENCES other(id)"},
						},
						ForeignKeysAdded: []schemalog.ForeignKey{
							{Name: "fk_new", Definition: "FOREIGN KEY (\"id\") REFERENCES other(id) ON DELETE CASCADE"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("fk_old")),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (\"id\") REFERENCES other(id) ON DELETE CASCADE", quotedTableName(testSchema, table1), pglib.QuoteIdentifier("fk_new")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - materialized view added and deleted",
			diff: &schemalog.Diff{
				MaterializedViewsRemoved: []schemalog.MaterializedView{
					{
						Name:       "mv_old",
						Definition: fmt.Sprintf("SELECT id FROM %s", quotedTableName(testSchema, table1)),
					},
				},
				MaterializedViewsAdded: []schemalog.MaterializedView{
					{
						Name:       "mv_new",
						Definition: fmt.Sprintf("SELECT id FROM %s", quotedTableName(testSchema, table2)),
						Indexes: []schemalog.Index{
							{
								Name:       "mv_new_idx",
								Definition: fmt.Sprintf("CREATE INDEX mv_new_idx ON %s (id)", pglib.QuoteQualifiedIdentifier(testSchema, "mv_new")),
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_old",
					sql:    fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", pglib.QuoteQualifiedIdentifier(testSchema, "mv_old")),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_new",
					sql:    fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s", pglib.QuoteQualifiedIdentifier(testSchema, "mv_new"), fmt.Sprintf("SELECT id FROM %s", quotedTableName(testSchema, table2))),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_new",
					sql:    fmt.Sprintf("CREATE INDEX IF NOT EXISTS mv_new_idx ON %s (id)", pglib.QuoteQualifiedIdentifier(testSchema, "mv_new")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - materialized view name changed",
			diff: &schemalog.Diff{
				MaterializedViewsChanged: []schemalog.MaterializedViewsDiff{
					{
						MaterializedViewName: "mv_new",
						NameChange: &schemalog.ValueChange[string]{
							Old: "mv_old",
							New: "mv_new",
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_new",
					sql:    fmt.Sprintf("ALTER MATERIALIZED VIEW IF EXISTS %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, "mv_old"), pglib.QuoteIdentifier("mv_new")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - materialized view indexes added and removed",
			diff: &schemalog.Diff{
				MaterializedViewsChanged: []schemalog.MaterializedViewsDiff{
					{
						MaterializedViewName: "mv_test",
						IndexesRemoved: []schemalog.Index{
							{Name: "mv_old_idx"},
						},
						IndexesAdded: []schemalog.Index{
							{
								Name:       "mv_new_idx",
								Columns:    []string{"id"},
								Definition: fmt.Sprintf("CREATE INDEX mv_new_idx ON %s (id)", pglib.QuoteQualifiedIdentifier(testSchema, "mv_test")),
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_test",
					sql:    fmt.Sprintf("DROP INDEX IF EXISTS %s", pglib.QuoteQualifiedIdentifier(testSchema, "mv_old_idx")),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_test",
					sql:    fmt.Sprintf("CREATE INDEX IF NOT EXISTS mv_new_idx ON %s (id)", pglib.QuoteQualifiedIdentifier(testSchema, "mv_test")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - materialized view indexes changed",
			diff: &schemalog.Diff{
				MaterializedViewsChanged: []schemalog.MaterializedViewsDiff{
					{
						MaterializedViewName: "mv_test",
						IndexesChanged: []string{
							fmt.Sprintf("ALTER INDEX %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, "mv_old_idx"), pglib.QuoteIdentifier("mv_new_idx")),
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  "mv_test",
					sql:    fmt.Sprintf("ALTER INDEX %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, "mv_old_idx"), pglib.QuoteIdentifier("mv_new_idx")),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ddlAdapter := newDDLAdapter(nil)

			queries, err := ddlAdapter.schemaDiffToQueries(testSchema, tc.diff)
			require.ErrorIs(t, err, tc.wantErr)
			require.ElementsMatch(t, tc.wantQueries, queries)
		})
	}
}

func TestDDLAdapter_buildColumnDefinition(t *testing.T) {
	t.Parallel()

	defaultValue := "42"
	seqDefaultValue := "nextval('test_seq'::regclass)"
	generatedExpression := "upper(name)"
	identityAlways := "a"
	identityByDefault := "d"
	generatedVirtual := "v"
	generatedStored := "s"

	tests := []struct {
		name   string
		column schemalog.Column
		want   string
	}{
		{
			name: "nullable column with no default",
			column: schemalog.Column{
				Name:     "name",
				DataType: "text",
				Nullable: true,
			},
			want: "\"name\" text",
		},
		{
			name: "not nullable column with no default",
			column: schemalog.Column{
				Name:     "id",
				DataType: "uuid",
				Nullable: false,
			},
			want: "\"id\" uuid NOT NULL",
		},
		{
			name: "column with default value",
			column: schemalog.Column{
				Name:         "age",
				DataType:     "int",
				Nullable:     false,
				DefaultValue: &defaultValue,
			},
			want: "\"age\" int NOT NULL DEFAULT 42",
		},
		{
			name: "column with sequence default value - serial",
			column: schemalog.Column{
				Name:         "id",
				DataType:     "integer",
				Nullable:     false,
				DefaultValue: &seqDefaultValue,
			},
			want: "\"id\" integer NOT NULL DEFAULT nextval('test_seq'::regclass)",
		},
		{
			name: "column with identity always",
			column: schemalog.Column{
				Name:     "id",
				DataType: "bigint",
				Nullable: false,
				Identity: identityAlways,
			},
			want: "\"id\" bigint NOT NULL GENERATED ALWAYS AS IDENTITY",
		},
		{
			name: "column with identity by default",
			column: schemalog.Column{
				Name:     "id",
				DataType: "bigint",
				Nullable: false,
				Identity: identityByDefault,
			},
			want: "\"id\" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY",
		},
		{
			name: "column with virtual generated column",
			column: schemalog.Column{
				Name:          "full_name",
				DataType:      "text",
				Nullable:      true,
				GeneratedKind: generatedVirtual,
				DefaultValue:  &generatedExpression,
			},
			want: "\"full_name\" text GENERATED ALWAYS AS (upper(name)) VIRTUAL",
		},
		{
			name: "column with stored generated column",
			column: schemalog.Column{
				Name:          "full_name",
				DataType:      "text",
				Nullable:      false,
				GeneratedKind: generatedStored,
				DefaultValue:  &generatedExpression,
			},
			want: "\"full_name\" text NOT NULL GENERATED ALWAYS AS (upper(name)) STORED",
		},
		{
			name: "nullable column with default value",
			column: schemalog.Column{
				Name:         "score",
				DataType:     "decimal",
				Nullable:     true,
				DefaultValue: &defaultValue,
			},
			want: "\"score\" decimal DEFAULT 42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ddlAdapter := newDDLAdapter(nil)
			result := ddlAdapter.buildColumnDefinition(&tc.column)
			require.Equal(t, tc.want, result)
		})
	}
}

func TestDDLAdapter_buildSequenceQueries(t *testing.T) {
	t.Parallel()

	seq1 := "seq1"
	seq2 := "seq2"
	dataType := "bigint"
	increment := "1"
	minValue := "1"
	maxValue := "9223372036854775807"
	startValue := "1"
	cycleYes := "YES"
	cycleNo := "NO"

	tests := []struct {
		name string
		diff *schemalog.Diff

		wantQueries     []*query
		wantDropQueries []*query
	}{
		{
			name: "ok - sequence removed",
			diff: &schemalog.Diff{
				SequencesRemoved: []schemalog.Sequence{
					{Name: seq1},
				},
			},

			wantDropQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
			wantQueries: []*query{},
		},
		{
			name: "ok - sequence added with minimal options",
			diff: &schemalog.Diff{
				SequencesAdded: []schemalog.Sequence{
					{Name: seq1},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence added with all options",
			diff: &schemalog.Diff{
				SequencesAdded: []schemalog.Sequence{
					{
						Name:         seq1,
						DataType:     &dataType,
						Increment:    &increment,
						MinimumValue: &minValue,
						MaximumValue: &maxValue,
						StartValue:   &startValue,
						CycleOption:  &cycleYes,
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CYCLE", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence added with no cycle option",
			diff: &schemalog.Diff{
				SequencesAdded: []schemalog.Sequence{
					{
						Name:        seq1,
						CycleOption: &cycleNo,
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s NO CYCLE", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with name change",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq2,
						NameChange: &schemalog.ValueChange[string]{
							Old: seq1,
							New: seq2,
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq2,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, seq1), pglib.QuoteIdentifier(seq2)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with data type change",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq1,
						DataTypeChange: &schemalog.ValueChange[*string]{
							Old: nil,
							New: &dataType,
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s AS bigint", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with increment change",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq1,
						IncrementChange: &schemalog.ValueChange[*string]{
							Old: &increment,
							New: func() *string { v := "2"; return &v }(),
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s INCREMENT BY 2", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with min/max value changes",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq1,
						MinimumValueChange: &schemalog.ValueChange[*string]{
							Old: &minValue,
							New: func() *string { v := "10"; return &v }(),
						},
						MaximumValueChange: &schemalog.ValueChange[*string]{
							Old: &maxValue,
							New: func() *string { v := "1000"; return &v }(),
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s MINVALUE 10 MAXVALUE 1000", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with start value change",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq1,
						StartValueChange: &schemalog.ValueChange[*string]{
							Old: &startValue,
							New: func() *string { v := "100"; return &v }(),
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s RESTART WITH 100", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with cycle option change",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq1,
						CycleOptionChange: &schemalog.ValueChange[*string]{
							Old: &cycleNo,
							New: &cycleYes,
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s CYCLE", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed from cycle to no cycle",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq1,
						CycleOptionChange: &schemalog.ValueChange[*string]{
							Old: &cycleYes,
							New: &cycleNo,
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s NO CYCLE", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - sequence changed with multiple properties and name change",
			diff: &schemalog.Diff{
				SequencesChanged: []schemalog.SequenceDiff{
					{
						SequenceName: seq2,
						NameChange: &schemalog.ValueChange[string]{
							Old: seq1,
							New: seq2,
						},
						DataTypeChange: &schemalog.ValueChange[*string]{
							Old: nil,
							New: &dataType,
						},
						IncrementChange: &schemalog.ValueChange[*string]{
							Old: nil,
							New: func() *string { v := "5"; return &v }(),
						},
					},
				},
			},

			wantDropQueries: []*query{},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq2,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s RENAME TO %s", pglib.QuoteQualifiedIdentifier(testSchema, seq1), pglib.QuoteIdentifier(seq2)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  seq2,
					sql:    fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s AS bigint INCREMENT BY 5", pglib.QuoteQualifiedIdentifier(testSchema, seq2)),
					isDDL:  true,
				},
			},
		},
		{
			name: "ok - multiple sequences added and removed",
			diff: &schemalog.Diff{
				SequencesRemoved: []schemalog.Sequence{
					{Name: seq1},
				},
				SequencesAdded: []schemalog.Sequence{
					{
						Name:        seq2,
						DataType:    &dataType,
						Increment:   &increment,
						CycleOption: &cycleNo,
					},
				},
			},

			wantDropQueries: []*query{
				{
					schema: testSchema,
					table:  seq1,
					sql:    fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", pglib.QuoteQualifiedIdentifier(testSchema, seq1)),
					isDDL:  true,
				},
			},
			wantQueries: []*query{
				{
					schema: testSchema,
					table:  seq2,
					sql:    fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s AS bigint INCREMENT BY 1 NO CYCLE", pglib.QuoteQualifiedIdentifier(testSchema, seq2)),
					isDDL:  true,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ddlAdapter := newDDLAdapter(nil)

			queries, dropQueries := ddlAdapter.buildSequenceQueries(testSchema, tc.diff)
			require.Equal(t, tc.wantQueries, queries)
			require.Equal(t, tc.wantDropQueries, dropQueries)
		})
	}
}
