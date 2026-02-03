// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestData_IsDDLEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data *Data
		want bool
	}{
		{
			name: "ddl event",
			data: &Data{
				Action: "M",
				Prefix: "pgstream.ddl",
			},
			want: true,
		},
		{
			name: "not a logical message",
			data: &Data{
				Action: "I",
			},
			want: false,
		},
		{
			name: "logical message with different prefix",
			data: &Data{
				Action: "M",
				Prefix: "other.prefix",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.data.IsDDLEvent()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestData_ParseDDLEvent(t *testing.T) {
	t.Parallel()

	defaultVal := "nextval('users_id_seq'::regclass)"
	identityVal := "ALWAYS"

	tests := []struct {
		name    string
		data    *Data
		want    *DDLEvent
		wantErr error
	}{
		{
			name: "parse create table event",
			data: &Data{
				Action: "M",
				Prefix: "pgstream.ddl",
				Content: `{
					"ddl": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
					"schema_name": "public",
					"command_tag": "CREATE TABLE",
					"objects": [{
						"type": "table",
						"identity": "public.users",
						"schema": "public",
						"oid": "16384",
						"pgstream_id": "ck7s8u4000001",
						"columns": [
							{
								"attnum": 1,
								"name": "id",
								"type": "integer",
								"nullable": false,
								"default": "nextval('users_id_seq'::regclass)",
								"generated": false,
								"identity": "ALWAYS",
								"unique": true
							},
							{
								"attnum": 2,
								"name": "name",
								"type": "text",
								"nullable": true,
								"generated": false,
								"unique": false
							}
						],
						"primary_key_columns": ["id"]
					}]
				}`,
			},
			want: &DDLEvent{
				DDL:        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
				SchemaName: "public",
				CommandTag: "CREATE TABLE",
				Objects: []DDLObject{
					{
						Type:       "table",
						Identity:   "public.users",
						Schema:     "public",
						OID:        "16384",
						PgstreamID: "ck7s8u4000001",
						Columns: []DDLColumn{
							{
								Attnum:    1,
								Name:      "id",
								Type:      "integer",
								Nullable:  false,
								Default:   &defaultVal,
								Generated: false,
								Identity:  &identityVal,
								Unique:    true,
							},
							{
								Attnum:    2,
								Name:      "name",
								Type:      "text",
								Nullable:  true,
								Generated: false,
								Unique:    false,
							},
						},
						PrimaryKeyColumns: []string{"id"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "not a ddl event",
			data: &Data{
				Action: "I",
			},
			wantErr: ErrNotDDLEvent,
		},
		{
			name: "invalid json content",
			data: &Data{
				Action:  "M",
				Prefix:  "pgstream.ddl",
				Content: "invalid json",
			},
			wantErr: ErrInvalidDDLEventContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := WalDataToDDLEvent(tt.data)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDDLEvent_GetTableObjects(t *testing.T) {
	t.Parallel()

	ddlEvent := &DDLEvent{
		Objects: []DDLObject{
			{Type: "table", Identity: "public.users"},
			{Type: "index", Identity: "public.users_idx"},
			{Type: "table", Identity: "public.posts"},
			{Type: "constraint", Identity: "public.users_pkey"},
		},
	}

	tables := ddlEvent.GetTableObjects()
	require.Len(t, tables, 2)
	require.Equal(t, "public.users", tables[0].Identity)
	require.Equal(t, "public.posts", tables[1].Identity)
}

func TestDDLObject_GetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity string
		want     string
	}{
		{
			name:     "qualified name",
			identity: "public.users",
			want:     "users",
		},
		{
			name:     "table column name",
			identity: "public.test_os.username",
			want:     "username",
		},
		{
			name:     "unqualified name",
			identity: "users",
			want:     "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			obj := &DDLObject{Identity: tt.identity}
			got := obj.GetName()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDDLObject_GetSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity string
		want     string
	}{
		{
			name:     "table",
			identity: "public.users",
			want:     "public",
		},
		{
			name:     "table column",
			identity: "public.test_os.username",
			want:     "public",
		},
		{
			name:     "custom schema",
			identity: "my_schema.test_table.col",
			want:     "my_schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			obj := &DDLObject{Identity: tt.identity}
			got := obj.GetSchema()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDDLObject_GetTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity string
		want     string
	}{
		{
			name:     "table object",
			identity: "public.users",
			want:     "users",
		},
		{
			name:     "table column object",
			identity: "public.test_os.username",
			want:     "test_os",
		},
		{
			name:     "unqualified",
			identity: "users",
			want:     "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			obj := &DDLObject{Identity: tt.identity}
			got := obj.GetTable()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDDLColumn_GetColumnPgstreamID(t *testing.T) {
	t.Parallel()

	col := &DDLColumn{
		Attnum: 3,
		Name:   "email",
	}

	pgstreamID := col.GetColumnPgstreamID("ck7s8u4000001")
	require.Equal(t, "ck7s8u4000001-3", pgstreamID)
}
