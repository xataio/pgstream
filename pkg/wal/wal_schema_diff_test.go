// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_DDLEventToSchemaDiff(t *testing.T) {
	t.Parallel()

	stringPtr := func(s string) *string { return &s }

	tests := []struct {
		name     string
		ddlEvent *DDLEvent
		wantDiff *SchemaDiff
		wantErr  error
	}{
		{
			name:     "nil event",
			ddlEvent: nil,
			wantDiff: &SchemaDiff{},
			wantErr:  nil,
		},
		{
			name: "DROP SCHEMA",
			ddlEvent: &DDLEvent{
				DDL:        "DROP SCHEMA public",
				SchemaName: "public",
				CommandTag: "DROP SCHEMA",
			},
			wantDiff: &SchemaDiff{
				SchemaName:    "public",
				SchemaDropped: true,
			},
			wantErr: nil,
		},
		{
			name: "DROP TABLE",
			ddlEvent: &DDLEvent{
				DDL:        "DROP TABLE public.users",
				SchemaName: "public",
				CommandTag: "DROP TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesRemoved: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "CREATE TABLE",
			ddlEvent: &DDLEvent{
				DDL:        "CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT)",
				SchemaName: "public",
				CommandTag: "CREATE TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
							{Attnum: 2, Name: "name", Type: "text", Nullable: true},
						},
						PrimaryKeyColumns: []string{"id"},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesAdded: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
							{Attnum: 2, Name: "name", Type: "text", Nullable: true},
						},
						PrimaryKeyColumns: []string{"id"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE ADD COLUMN",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users ADD COLUMN age INT",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
							{Attnum: 2, Name: "name", Type: "text", Nullable: true},
							{Attnum: 3, Name: "age", Type: "integer", Nullable: true},
						},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstream-id-1",
						ColumnsAdded: []DDLColumn{
							{Attnum: 3, Name: "age", Type: "integer", Nullable: true},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE ADD COLUMN with quoted name",
			ddlEvent: &DDLEvent{
				DDL:        `ALTER TABLE public.users ADD COLUMN "user age" INT`,
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "user age", Type: "integer", Nullable: true},
						},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstream-id-1",
						ColumnsAdded: []DDLColumn{
							{Attnum: 1, Name: "user age", Type: "integer", Nullable: true},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE DROP COLUMN",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users DROP COLUMN age",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
							{Attnum: 2, Name: "name", Type: "text", Nullable: true},
						},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstream-id-1",
						ColumnsRemoved: []DDLColumn{
							{Name: "age"},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE ALTER COLUMN TYPE",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users ALTER COLUMN age TYPE BIGINT",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 3, Name: "age", Type: "bigint", Nullable: true},
						},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstream-id-1",
						ColumnsChanged: []ColumnDiff{
							{
								ColumnName: "age",
								TypeChange: &ValueChange[string]{Old: "", New: "bigint"},
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE RENAME COLUMN",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users RENAME COLUMN age TO user_age",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 3, Name: "user_age", Type: "integer", Nullable: true},
						},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstream-id-1",
						ColumnsChanged: []ColumnDiff{
							{
								ColumnName:       "user_age",
								ColumnPgstreamID: "pgstream-id-1-3",
								NameChange:       &ValueChange[string]{Old: "age", New: "user_age"},
							},
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE RENAME TO",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users RENAME TO customers",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.customers",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:       "customers",
						TablePgstreamID: "pgstream-id-1",
						TableNameChange: &ValueChange[string]{Old: "users", New: "customers"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE ADD CONSTRAINT PRIMARY KEY",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id)",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:              "table",
						Identity:          "public.users",
						Schema:            "public",
						OID:               "12345",
						PgstreamID:        "pgstream-id-1",
						PrimaryKeyColumns: []string{"id"},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesChanged: []TableDiff{
					{
						TableName:             "users",
						TablePgstreamID:       "pgstream-id-1",
						TablePrimaryKeyChange: &ValueChange[[]string]{Old: nil, New: []string{"id"}},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "CREATE TABLE with identity column",
			ddlEvent: &DDLEvent{
				DDL:        "CREATE TABLE public.users (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)",
				SchemaName: "public",
				CommandTag: "CREATE TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "id", Type: "integer", Nullable: false, Identity: stringPtr("ALWAYS")},
							{Attnum: 2, Name: "name", Type: "text", Nullable: true},
						},
						PrimaryKeyColumns: []string{"id"},
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
				TablesAdded: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
						Columns: []DDLColumn{
							{Attnum: 1, Name: "id", Type: "integer", Nullable: false, Identity: stringPtr("ALWAYS")},
							{Attnum: 2, Name: "name", Type: "text", Nullable: true},
						},
						PrimaryKeyColumns: []string{"id"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "ALTER TABLE with no recognized operation",
			ddlEvent: &DDLEvent{
				DDL:        "ALTER TABLE public.users SET SCHEMA other_schema",
				SchemaName: "public",
				CommandTag: "ALTER TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "other_schema.users",
						Schema:     "other_schema",
						OID:        "12345",
						PgstreamID: "pgstream-id-1",
					},
				},
			},
			wantDiff: &SchemaDiff{
				SchemaName: "public",
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			schemaDiff, err := DDLEventToSchemaDiff(tc.ddlEvent)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantDiff, schemaDiff)
		})
	}
}
