// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestDMLAdapter_walDataToQueries(t *testing.T) {
	t.Parallel()

	testTableID := xid.New()
	testTable := "table"
	testSchema := "test"
	quotedTestTable := quotedTableName(testSchema, testTable)
	quotedColumnNames := []string{`"id"`, `"name"`}
	columnID := func(i int) string {
		return fmt.Sprintf("%s-%d", testTableID, i)
	}

	now := time.Now()

	tests := []struct {
		name                  string
		walData               *wal.Data
		action                onConflictAction
		generatedColumns      map[string]struct{}
		alwaysIdentityColumns map[string]struct{}
		sequenceColumns       map[string]string
		forCopy               bool

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "truncate",
			walData: &wal.Data{
				Action: "T",
				Schema: testSchema,
				Table:  testTable,
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("TRUNCATE %s", quotedTestTable),
				},
			},
		},
		{
			name: "delete with simple primary key",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1", quotedTestTable),
					args:   []any{1},
				},
			},
		},
		{
			name: "delete with composite primary key",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1), columnID(2)},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1 AND \"name\" = $2", quotedTestTable),
					args:   []any{1, "alice"},
				},
			},
		},
		{
			name: "delete with full identity",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1 AND \"name\" = $2", quotedTestTable),
					args:   []any{1, "alice"},
				},
			},
		},
		{
			name: "delete - full identity and null column",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "null_column", Value: nil},
					{ID: columnID(2), Name: "id", Value: 1},
					{ID: columnID(3), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("DELETE FROM %s WHERE \"null_column\" IS NULL AND \"id\" = $1 AND \"name\" = $2", quotedTestTable),
					args:   []any{1, "alice"},
				},
			},
		},
		{
			name: "error - delete",
			walData: &wal.Data{
				Action:   "D",
				Schema:   testSchema,
				Table:    testTable,
				Identity: []wal.Column{},
				Metadata: wal.Metadata{},
			},

			wantQueries: nil,
			wantErr:     errUnableToBuildQuery,
		},
		{
			name: "insert",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{1, "alice"},
				},
			},
		},
		{
			name: "insert with sequences",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: float64(1)},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			sequenceColumns: map[string]string{
				`"id"`: `"id_seq"`,
			},
			forCopy: false,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{float64(1), "alice"},
				},
				{
					schema: testSchema,
					table:  testTable,
					sql:    "SELECT setval($1::regclass, $2::bigint, true)",
					args:   []any{`"id_seq"`, int64(1)},
				},
			},
		},
		{
			name: "insert with int64 sequence value preserves precision above 2^53",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: int64(9007199254740993)},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			sequenceColumns: map[string]string{
				`"id"`: `"id_seq"`,
			},
			forCopy: false,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{int64(9007199254740993), "alice"},
				},
				{
					schema: testSchema,
					table:  testTable,
					sql:    "SELECT setval($1::regclass, $2::bigint, true)",
					args:   []any{`"id_seq"`, int64(9007199254740993)},
				},
			},
		},
		{
			name: "insert with sequences - for copy enabled",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: float64(1)},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			sequenceColumns: map[string]string{
				`"id"`: `"id_seq"`,
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{float64(1), "alice"},
				},
			},
		},
		{
			name: "insert with sequences - invalid column value",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			sequenceColumns: map[string]string{
				`"name"`: `"name_seq"`,
			},
			forCopy: false,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{1, "alice"},
				},
			},
		},
		{
			name: "insert with infinity timestamp",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "created_at", Value: pgtype.Infinity, Type: "timestamptz"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"name"`, `"created_at"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"created_at\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args:        []any{1, "alice", pgtype.Timestamptz{Valid: true, InfinityModifier: pgtype.Infinity}},
				},
			},
		},
		{
			name: "insert with tstzrange",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "datetime_range", Value: pgtype.Range[any]{
						Lower:     now.Add(-1 * time.Minute),
						Upper:     now.Add(time.Minute),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					}, Type: "tstzrange"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"name"`, `"datetime_range"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"datetime_range\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args: []any{1, "alice", pgtype.Range[time.Time]{
						Lower:     now.Add(-1 * time.Minute),
						Upper:     now.Add(time.Minute),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					}},
				},
			},
		},
		{
			name: "insert with tstzrange will no upper value",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "datetime_range", Value: pgtype.Range[any]{
						Lower:     now.Add(-1 * time.Minute),
						Upper:     nil,
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					}, Type: "tstzrange"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"name"`, `"datetime_range"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"datetime_range\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args: []any{1, "alice", pgtype.Range[time.Time]{
						Lower:     now.Add(-1 * time.Minute),
						Upper:     time.Time{},
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					}},
				},
			},
		},
		{
			name: "insert with tstzrange will no lower value",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "datetime_range", Value: pgtype.Range[any]{
						Lower:     nil,
						Upper:     now.Add(time.Minute),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					}, Type: "tstzrange"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"name"`, `"datetime_range"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"datetime_range\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args: []any{1, "alice", pgtype.Range[time.Time]{
						Lower:     time.Time{},
						Upper:     now.Add(time.Minute),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					}},
				},
			},
		},
		{
			name: "insert with tsvector - for copy enabled",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "search_vec", Value: []byte("'hello':1 'world':2"), Type: "tsvector"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"search_vec"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"search_vec\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{1, "'hello':1 'world':2"},
				},
			},
		},
		{
			name: "insert with tsvector string - for copy enabled",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "search_vec", Value: "'hello':1 'world':2", Type: "tsvector"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"search_vec"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"search_vec\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{1, "'hello':1 'world':2"},
				},
			},
		},
		{
			name: "insert with enum array - for copy enabled",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "status_array", Value: "{EXAMPLE}", Type: "text[]"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"name"`, `"status_array"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"status_array\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args:        []any{1, "alice", []string{"EXAMPLE"}},
				},
			},
		},
		{
			// pgx has no entry for the array of a user-defined enum, so the
			// type name alone routes the batch to text-format COPY and the
			// postgres array literal is kept for the target to parse, without
			// the column having to be listed in enumColumns.
			name: "insert with enum array using underscore prefix - for copy enabled",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "status_array", Value: "{EXAMPLE}", Type: "_ExampleEnum"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			forCopy: true,

			wantQueries: []*query{
				{
					schema:        testSchema,
					table:         testTable,
					columnNames:   []string{`"id"`, `"name"`, `"status_array"`},
					needsTextCopy: true,
					sql:           fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"status_array\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args:          []any{1, "alice", "{EXAMPLE}"},
				},
			},
		},
		{
			name: "insert - on conflict do nothing",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			action: onConflictDoNothing,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2) ON CONFLICT DO NOTHING", quotedTestTable),
					args:        []any{1, "alice"},
				},
			},
		},
		{
			name: "insert - on conflict do update",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			action: onConflictUpdate,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"id\" = EXCLUDED.\"id\", \"name\" = EXCLUDED.\"name\"", quotedTestTable),
					args:        []any{1, "alice"},
				},
			},
		},
		{
			name: "insert - on conflict do update with composite primary key",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1), columnID(2)},
				},
			},
			action: onConflictUpdate,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2) ON CONFLICT (\"id\",\"name\") DO UPDATE SET \"id\" = EXCLUDED.\"id\", \"name\" = EXCLUDED.\"name\"", quotedTestTable),
					args:        []any{1, "alice"},
				},
			},
		},
		{
			name: "insert - on conflict do update without PK",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
			},
			action: onConflictUpdate,

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: quotedColumnNames,
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
					args:        []any{1, "alice"},
				},
			},
		},
		{
			name: "update - primary key",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
					args:   []any{1, "alice", 1},
				},
			},
		},
		{
			name: "update - default identity",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
					args:   []any{1, "alice", 1},
				},
			},
		},
		{
			name: "update - full identity",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "a"},
				},
				Metadata: wal.Metadata{},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3 AND \"name\" = $4", quotedTestTable),
					args:   []any{1, "alice", 1, "a"},
				},
			},
		},
		{
			name: "update - full identity and null column",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "null_column", Value: nil},
					{ID: columnID(4), Name: "age", Value: "20"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "a"},
					{ID: columnID(3), Name: "null_column", Value: nil},
					{ID: columnID(4), Name: "age", Value: "20"},
				},
				Metadata: wal.Metadata{},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2, \"null_column\" = $3, \"age\" = $4 WHERE \"id\" = $5 AND \"name\" = $6 AND \"null_column\" IS NULL AND \"age\" = $7", quotedTestTable),
					args:   []any{1, "alice", nil, "20", 1, "a", "20"},
				},
			},
		},
		{
			// regression: previously the always-identity column was included
			// in the SET clause, which Postgres rejects with
			// "column ... can only be updated to DEFAULT".
			name: "update - with always-identity column filtered from SET",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "request_id", Value: 42},
					{ID: columnID(3), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{},
			},
			alwaysIdentityColumns: map[string]struct{}{`"request_id"`: {}},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
					args:   []any{1, "alice", 1},
				},
			},
		},
		{
			// always-identity columns are kept in INSERTs since OVERRIDING
			// SYSTEM VALUE lets Postgres accept the explicit value.
			name: "insert - with always-identity column kept",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "request_id", Value: 42},
					{ID: columnID(3), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{},
			},
			alwaysIdentityColumns: map[string]struct{}{`"request_id"`: {}},

			wantQueries: []*query{
				{
					schema:      testSchema,
					table:       testTable,
					columnNames: []string{`"id"`, `"request_id"`, `"name"`},
					sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"request_id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
					args:        []any{1, 42, "alice"},
				},
			},
		},
		{
			name: "update - with generated column",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "generated_col", Value: "gen_value"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{},
			},
			generatedColumns: map[string]struct{}{`"generated_col"`: {}},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  testTable,
					sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
					args:   []any{1, "alice", 1},
				},
			},
		},
		{
			name: "error - update",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{},
				Metadata: wal.Metadata{},
			},

			wantQueries: nil,
			wantErr:     errUnableToBuildQuery,
		},
		{
			name: "unknown",
			walData: &wal.Data{
				Action: "X",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQueries: []*query{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &dmlAdapter{
				logger:           log.NewNoopLogger(),
				onConflictAction: tc.action,
				forCopy:          tc.forCopy,
				pgTypeMap:        pgtype.NewMap(),
			}
			queries, err := a.walDataToQueries(tc.walData, schemaInfo{
				generatedColumns:      tc.generatedColumns,
				alwaysIdentityColumns: tc.alwaysIdentityColumns,
				sequenceColumns:       tc.sequenceColumns,
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func Test_needsTextCopyForColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		columnNames []string
		columnTypes []string
		enumColumns map[string]enumColumn

		want bool
	}{
		{
			// the names Mapper.TypeForOID resolves
			name:        "no text-only columns",
			columnNames: []string{`"id"`, `"name"`},
			columnTypes: []string{"int4", "text"},
			enumColumns: nil,
			want:        false,
		},
		{
			name:        "text-only extension type",
			columnNames: []string{`"id"`, `"location"`},
			columnTypes: []string{"int4", "ltree"},
			enumColumns: nil,
			want:        true,
		},
		{
			// regression for #1211: a PostGIS geometry reaches the writer as
			// the hex EWKB text, and binary COPY made the target read its
			// first character as the byte order flag
			name:        "postgis geometry column",
			columnNames: []string{`"id"`, `"geom"`},
			columnTypes: []string{"int4", "geometry"},
			enumColumns: nil,
			want:        true,
		},
		{
			name:        "array of an extension type",
			columnNames: []string{`"id"`, `"geoms"`},
			columnTypes: []string{"int4", "_geometry"},
			enumColumns: nil,
			want:        true,
		},
		{
			name:        "extension type pgstream registers a binary codec for",
			columnNames: []string{`"id"`, `"embedding"`, `"attrs"`},
			columnTypes: []string{"int4", "vector", "hstore"},
			enumColumns: nil,
			want:        false,
		},
		{
			// the snapshot never spells a type this way
			name:        "format_type spelling falls back to text copy",
			columnNames: []string{`"id"`, `"name"`},
			columnTypes: []string{"int4", "character varying"},
			enumColumns: nil,
			want:        true,
		},
		{
			name:        "timetz column",
			columnNames: []string{`"id"`, `"start_at"`},
			columnTypes: []string{"int4", "timetz"},
			enumColumns: nil,
			want:        true,
		},
		{
			name:        "timetz column, format_type spelling",
			columnNames: []string{`"id"`, `"start_at"`},
			columnTypes: []string{"int4", "time with time zone"},
			enumColumns: nil,
			want:        true,
		},
		{
			name:        "enum column",
			columnNames: []string{`"id"`, `"mood"`},
			columnTypes: []string{"int4", "mood"},
			enumColumns: map[string]enumColumn{`"mood"`: {enumType: "public.mood"}},
			want:        true,
		},
		{
			name:        "enum type present but column filtered out",
			columnNames: []string{`"id"`},
			columnTypes: []string{"int4"},
			enumColumns: map[string]enumColumn{`"mood"`: {enumType: "public.mood"}},
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, newTestDMLAdapterForCopy(t).needsTextCopyForColumns(tc.columnNames, tc.columnTypes, tc.enumColumns))
		})
	}
}

func Test_needsTextCopyForType(t *testing.T) {
	t.Parallel()

	a := newTestDMLAdapterForCopy(t)

	// pgx's static type map has no entry for these, so they reach the writer as
	// their text representation and only text-format COPY can deliver them.
	for _, colType := range []string{
		"geometry", "geography", "_geometry", "citext", "money", "tsquery",
		"cube", "ltree", "timetz", "my_composite",
		// the snapshot never spells a type the way format_type prints it
		"integer", "bigint", "character varying", "double precision",
		"timestamp without time zone", "time with time zone", "character varying[]",
	} {
		require.Truef(t, a.needsTextCopyForType(colType), "expected text copy for %q", colType)
	}

	// built-in types, their format_type spellings, their array forms, and the
	// extension types pgstream registers a binary codec for all stay on the
	// faster binary COPY.
	for _, colType := range []string{
		"int4", "int8", "text", "varchar", "numeric", "uuid",
		"jsonb", "tsvector", "int4range", "_text", "text[]",
		"hstore", "vector", "halfvec",
		// a column with no resolved type name keeps the binary path
		"",
	} {
		require.Falsef(t, a.needsTextCopyForType(colType), "expected binary copy for %q", colType)
	}
}

// Test_copyFormatSets_coverRegisteredExtensionTypes pins the COPY-format sets
// to the registry of extension types pgstream teaches pgx about, so a new
// registration cannot silently end up in neither set.
func Test_copyFormatSets_coverRegisteredExtensionTypes(t *testing.T) {
	t.Parallel()

	for _, name := range pglib.ExtensionTypeNames() {
		_, textOnly := textOnlyCopyTypes[name]
		_, binarySafe := binaryCopySafeTypes[name]
		require.NotEqualf(t, textOnly, binarySafe,
			"extension type %q must be either text-only or binary-copy safe", name)
	}

	require.Contains(t, textOnlyCopyTypes, "ltree")
	require.Contains(t, binaryCopySafeTypes, "vector")
}

func Test_updateValueForCopy_enumArray(t *testing.T) {
	t.Parallel()

	a := newTestDMLAdapterForCopy(t)

	// A non-enum array is parsed into a Go slice so pgx's binary COPY encoder
	// can handle it.
	require.Equal(t, []string{"a", "b"}, a.updateValueForCopy("{a,b}", "text[]", false))

	// An array of a user-defined enum goes out through text-format COPY, which
	// writes the postgres array literal verbatim — parsing it into a slice here
	// would leave the text encoder with a value it cannot render.
	require.Equal(t, "{happy,sad}", a.updateValueForCopy("{happy,sad}", "mood[]", true))

	// The same holds for an array of an extension type pgx has no binary codec
	// for, which the caller flags from the column type alone.
	require.Equal(t, "{0101000000,0101000020}",
		a.updateValueForCopy("{0101000000,0101000020}", "_geometry", a.needsTextCopyForType("_geometry")))
}

func Test_updateValueForCopy_arrayDimensions(t *testing.T) {
	t.Parallel()

	a := newTestDMLAdapterForCopy(t)

	// An array of one dimension becomes a Go slice, which is what pgx's
	// binary COPY encoder takes.
	require.Equal(t, []string{"1", "2", "3"}, a.updateValueForCopy("{1,2,3}", "int4[]", false))

	// An array of more than one dimension keeps its dimensions. A flat slice
	// here writes {{1,2},{3,4}} to the target as {1,2,3,4}: the shape is gone,
	// no error is raised, and nothing counts it.
	nested, ok := a.updateValueForCopy("{{1,2},{3,4}}", "int4[]", false).(pgtype.Array[string])
	require.True(t, ok, "a nested array has to keep a type that carries its dimensions")
	require.Equal(t, []string{"1", "2", "3", "4"}, nested.Elements)
	require.Equal(t, []pgtype.ArrayDimension{
		{Length: 2, LowerBound: 1},
		{Length: 2, LowerBound: 1},
	}, nested.Dims)
}

func Test_updateValueForCopy_rangeLiteral(t *testing.T) {
	t.Parallel()

	a := newTestDMLAdapterForCopy(t)

	// a range literal produced by a transformer is parsed into the typed
	// range pgx's binary COPY encoder can handle
	require.Equal(t,
		pgtype.Range[int32]{Lower: 1, Upper: 10, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		a.updateValueForCopy("[1,10)", "int4range", false))
	require.Equal(t,
		pgtype.Range[int64]{Lower: 5, LowerType: pgtype.Exclusive, UpperType: pgtype.Unbounded, Valid: true},
		a.updateValueForCopy("(5,)", "int8range", false))
	require.Equal(t,
		pgtype.Range[pgtype.Date]{
			Lower:     pgtype.Date{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Date{Time: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true,
		},
		a.updateValueForCopy("[2024-01-01,2024-02-01)", "daterange", false))
	require.Equal(t,
		pgtype.Range[int32]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
		a.updateValueForCopy("empty", "int4range", false))

	// a literal that does not parse is left for pgx to report
	require.Equal(t, "not a range", a.updateValueForCopy("not a range", "int4range", false))
	// a text column is never touched
	require.Equal(t, "[1,10)", a.updateValueForCopy("[1,10)", "text", false))
}

func Test_newDMLAdapter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action string

		wantErr error
	}{
		{
			action:  "update",
			wantErr: nil,
		},
		{
			action:  "nothing",
			wantErr: nil,
		},
		{
			action:  "error",
			wantErr: nil,
		},
		{
			action:  "",
			wantErr: nil,
		},
		{
			action:  "invalid",
			wantErr: errUnsupportedOnConflictAction,
		},
	}

	for _, tc := range tests {
		t.Run(tc.action, func(t *testing.T) {
			t.Parallel()

			_, err := newDMLAdapter(tc.action, false, log.NewNoopLogger())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestDMLAdapter_filterRowColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		generatedColumns      map[string]struct{}
		alwaysIdentityColumns map[string]struct{}
		forUpdate             bool
		columns               []wal.Column

		wantColumns []string
		wantValues  []any
	}{
		{
			name:             "no generated columns",
			generatedColumns: map[string]struct{}{},
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},

			wantColumns: []string{`"id"`, `"name"`},
			wantValues:  []any{1, "alice"},
		},
		{
			name:             "with generated column",
			generatedColumns: map[string]struct{}{`"id"`: {}},
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
				{Name: "age", Value: 30},
			},

			wantColumns: []string{`"name"`, `"age"`},
			wantValues:  []any{"alice", 30},
		},
		{
			name:             "unknown generated columns",
			generatedColumns: map[string]struct{}{`"age"`: {}},
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},

			wantColumns: []string{`"id"`, `"name"`},
			wantValues:  []any{1, "alice"},
		},
		{
			// always-identity columns are NOT filtered for INSERT — the
			// builder uses OVERRIDING SYSTEM VALUE.
			name:                  "always-identity kept for insert",
			alwaysIdentityColumns: map[string]struct{}{`"id"`: {}},
			forUpdate:             false,
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},

			wantColumns: []string{`"id"`, `"name"`},
			wantValues:  []any{1, "alice"},
		},
		{
			// always-identity columns ARE filtered for UPDATE — Postgres
			// rejects explicit values in SET for GENERATED ALWAYS columns.
			name:                  "always-identity filtered for update",
			alwaysIdentityColumns: map[string]struct{}{`"id"`: {}},
			forUpdate:             true,
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},

			wantColumns: []string{`"name"`},
			wantValues:  []any{"alice"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := dmlAdapter{}
			rowColumns, _, rowValues := a.filterRowColumnsForAction(tc.columns, schemaInfo{
				generatedColumns:      tc.generatedColumns,
				alwaysIdentityColumns: tc.alwaysIdentityColumns,
			}, tc.forUpdate)
			require.Equal(t, tc.wantColumns, rowColumns)
			require.Equal(t, tc.wantValues, rowValues)
		})
	}
}
