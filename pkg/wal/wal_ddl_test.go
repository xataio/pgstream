// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"testing"

	jsonlib "github.com/xataio/pgstream/internal/json"

	"github.com/stretchr/testify/require"
)

const testDDLContent = `{
	"ddl": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
	"schema_name": "public",
	"command_tag": "CREATE TABLE",
	"objects": [{
		"type": "table",
		"identity": "public.users",
		"schema": "public",
		"oid": "16384",
		"pgstream_id": "ck7s8u4000001"
	}]
}`

func TestWalDataToDDLEvent_memoisation(t *testing.T) {
	t.Parallel()

	data := &Data{
		Action:  "M",
		Prefix:  "pgstream.ddl",
		Content: testDDLContent,
	}

	first, err := WalDataToDDLEvent(data)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := WalDataToDDLEvent(data)
	require.NoError(t, err)

	// repeat calls must return the exact same cached pointer, i.e. the content
	// is parsed only once.
	require.Same(t, first, second)

	// the error path is memoised too.
	notDDL := &Data{Action: "I"}
	_, err1 := WalDataToDDLEvent(notDDL)
	_, err2 := WalDataToDDLEvent(notDDL)
	require.ErrorIs(t, err1, ErrNotDDLEvent)
	require.ErrorIs(t, err2, ErrNotDDLEvent)
}

func BenchmarkWalDataToDDLEvent_memoised(b *testing.B) {
	data := &Data{
		Action:  "M",
		Prefix:  "pgstream.ddl",
		Content: testDDLContent,
	}
	// prime the memo so the benchmarked calls only hit the cached fast path.
	if _, err := WalDataToDDLEvent(data); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := WalDataToDDLEvent(data); err != nil {
			b.Fatal(err)
		}
	}
}

func TestWalDataToDDLEvent_serialisationUnaffected(t *testing.T) {
	t.Parallel()

	// a Data whose DDL event has been memoised must serialise byte-identically
	// to one that has not: the memo fields are unexported and excluded from JSON.
	plain := &Data{
		Action:  "M",
		Prefix:  "pgstream.ddl",
		Content: testDDLContent,
		Schema:  "public",
	}
	memoised := &Data{
		Action:  "M",
		Prefix:  "pgstream.ddl",
		Content: testDDLContent,
		Schema:  "public",
	}
	_, err := WalDataToDDLEvent(memoised)
	require.NoError(t, err)

	plainBytes, err := jsonlib.Marshal(plain)
	require.NoError(t, err)
	memoisedBytes, err := jsonlib.Marshal(memoised)
	require.NoError(t, err)

	require.Equal(t, plainBytes, memoisedBytes)
}

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

func TestDDLEvent_GetTableObjectByName(t *testing.T) {
	t.Parallel()

	ddlEvent := &DDLEvent{
		Objects: []DDLObject{
			{Type: "table", Identity: "public.users", Schema: "public"},
			{Type: "table", Identity: "public.posts", Schema: "public"},
			{Type: "table", Identity: "schema2.orders", Schema: "schema2"},
			{Type: "index", Identity: "public.users_idx", Schema: "public"},
		},
	}

	tests := []struct {
		name   string
		schema string
		table  string
		want   *DDLObject
	}{
		{
			name:   "existing table in public schema",
			schema: "public",
			table:  "users",
			want:   &DDLObject{Type: "table", Identity: "public.users", Schema: "public"},
		},
		{
			name:   "existing table in custom schema",
			schema: "schema2",
			table:  "orders",
			want:   &DDLObject{Type: "table", Identity: "schema2.orders", Schema: "schema2"},
		},
		{
			name:   "non-existing table",
			schema: "public",
			table:  "nonexistent",
			want:   nil,
		},
		{
			name:   "wrong schema",
			schema: "wrong_schema",
			table:  "users",
			want:   nil,
		},
		{
			name:   "empty schema",
			schema: "",
			table:  "users",
			want:   nil,
		},
		{
			name:   "empty table",
			schema: "public",
			table:  "",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ddlEvent.GetTableObjectByName(tt.schema, tt.table)
			require.Equal(t, tt.want, got)
		})
	}
}
