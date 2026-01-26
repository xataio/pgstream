// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	jsonlib "github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func TestPGSnapshotWALRestore_restoreToWAL(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	tests := []struct {
		name      string
		dump      []byte
		processor *mocks.Processor
		querier   *pgmocks.Querier

		wantProcessedEvents int
		wantErr             error
	}{
		{
			name: "ok - single CREATE TABLE statement",
			dump: []byte(`CREATE TABLE public.users (
    id integer PRIMARY KEY,
    name text
);`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 1,
			wantErr:             nil,
		},
		{
			name: "ok - multiple DDL statements",
			dump: []byte(`CREATE TABLE public.users (id integer);
CREATE TABLE public.orders (id integer);
ALTER TABLE public.users ADD COLUMN email text;
DROP TABLE public.temp;`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 4,
			wantErr:             nil,
		},
		{
			name: "ok - multi-line statement",
			dump: []byte(`CREATE TABLE public.users (
    id integer PRIMARY KEY,
    name text NOT NULL,
    email text,
    created_at timestamp DEFAULT now()
);`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 1,
			wantErr:             nil,
		},
		{
			name: "ok - statements with comments",
			dump: []byte(`-- This is a comment
CREATE TABLE public.users (id integer);
-- Another comment
CREATE TABLE public.orders (id integer);`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 2,
			wantErr:             nil,
		},
		{
			name: "ok - statements with empty lines",
			dump: []byte(`
CREATE TABLE public.users (id integer);


CREATE TABLE public.orders (id integer);

`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 2,
			wantErr:             nil,
		},
		{
			name: "ok - non-DDL statements ignored",
			dump: []byte(`INSERT INTO users VALUES (1, 'alice');
CREATE TABLE public.orders (id integer);
SELECT * FROM users;`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 1, // Only the CREATE TABLE
			wantErr:             nil,
		},
		{
			name: "error - processor fails",
			dump: []byte(`CREATE TABLE public.users (id integer);`),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return errTest },
			},
			querier: newNoTableQuerier(),
			wantErr: errTest,
		},
		{
			name: "ok - various DDL types",
			dump: []byte(`CREATE INDEX idx_users ON public.users(id);
CREATE SEQUENCE public.user_seq;
DROP INDEX public.idx_old;
ALTER TABLE public.users ADD CONSTRAINT pk_users PRIMARY KEY (id);
GRANT SELECT ON public.users TO readonly;`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 5,
			wantErr:             nil,
		},
		{
			name:                "ok - empty dump",
			dump:                []byte(``),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 0,
			wantErr:             nil,
		},
		{
			name: "ok - only comments and empty lines",
			dump: []byte(`-- Just comments

-- No DDL here
`),
			processor:           &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:             newNoTableQuerier(),
			wantProcessedEvents: 0,
			wantErr:             nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			restore := newPGSnapshotWALRestore(tc.processor, tc.querier)

			_, err := restore.restoreToWAL(context.Background(), pglib.PGRestoreOptions{}, tc.dump)

			if tc.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint(tc.wantProcessedEvents), tc.processor.GetProcessCalls())
			}
		})
	}
}

func TestPGSnapshotWALRestore_processDDLStatement(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	tests := []struct {
		name      string
		statement string
		timestamp string
		processor *mocks.Processor
		querier   *pgmocks.Querier

		wantCommandTag string
		wantSchema     string
		wantObjects    int
		wantErr        error
	}{
		{
			name:      "ok - CREATE TABLE with schema",
			statement: `CREATE TABLE public.users (id integer PRIMARY KEY);`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.NotNil(t, walEvent)
					require.NotNil(t, walEvent.Data)
					require.Equal(t, wal.LogicalMessageAction, walEvent.Data.Action)
					require.Equal(t, wal.DDLPrefix, walEvent.Data.Prefix)
					require.Equal(t, "2024-01-01T00:00:00Z", walEvent.Data.Timestamp)

					var ddlEvent wal.DDLEvent
					err := jsonlib.Unmarshal([]byte(walEvent.Data.Content), &ddlEvent)
					require.NoError(t, err)
					require.Equal(t, "CREATE TABLE", ddlEvent.CommandTag)
					require.Equal(t, "public", ddlEvent.SchemaName)
					require.Contains(t, ddlEvent.DDL, "CREATE TABLE public.users")
					return nil
				},
			},
			querier:        newNoTableQuerier(),
			wantCommandTag: "CREATE TABLE",
			wantSchema:     "public",
			wantObjects:    1,
			wantErr:        nil,
		},
		{
			name:      "ok - CREATE TABLE without schema (defaults to public)",
			statement: `CREATE TABLE users (id integer);`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					var ddlEvent wal.DDLEvent
					err := jsonlib.Unmarshal([]byte(walEvent.Data.Content), &ddlEvent)
					require.NoError(t, err)
					require.Equal(t, "public", ddlEvent.SchemaName)
					return nil
				},
			},
			querier:     newNoTableQuerier(),
			wantSchema:  "public",
			wantObjects: 1,
			wantErr:     nil,
		},
		{
			name:      "ok - ALTER TABLE",
			statement: `ALTER TABLE public.users ADD COLUMN email text;`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					var ddlEvent wal.DDLEvent
					err := jsonlib.Unmarshal([]byte(walEvent.Data.Content), &ddlEvent)
					require.NoError(t, err)
					require.Equal(t, "ALTER TABLE", ddlEvent.CommandTag)
					return nil
				},
			},
			querier:        newNoTableQuerier(),
			wantCommandTag: "ALTER TABLE",
			wantObjects:    1,
			wantErr:        nil,
		},
		{
			name:      "ok - DROP TABLE",
			statement: `DROP TABLE IF EXISTS public.users;`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					var ddlEvent wal.DDLEvent
					err := jsonlib.Unmarshal([]byte(walEvent.Data.Content), &ddlEvent)
					require.NoError(t, err)
					require.Equal(t, "DROP TABLE", ddlEvent.CommandTag)
					return nil
				},
			},
			querier:        newNoTableQuerier(), // Table doesn't exist (was dropped)
			wantCommandTag: "DROP TABLE",
			wantObjects:    1, // Should still add minimal object info
			wantErr:        nil,
		},
		{
			name:      "ok - CREATE INDEX",
			statement: `CREATE INDEX idx_users ON public.users(id);`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					var ddlEvent wal.DDLEvent
					err := jsonlib.Unmarshal([]byte(walEvent.Data.Content), &ddlEvent)
					require.NoError(t, err)
					require.Equal(t, "CREATE INDEX", ddlEvent.CommandTag)
					return nil
				},
			},
			querier:        newNoTableQuerier(),
			wantCommandTag: "CREATE INDEX",
			wantErr:        nil,
		},
		{
			name:      "ok - statement with no objects extracted",
			statement: `GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					var ddlEvent wal.DDLEvent
					err := jsonlib.Unmarshal([]byte(walEvent.Data.Content), &ddlEvent)
					require.NoError(t, err)
					require.Equal(t, "GRANT", ddlEvent.CommandTag)
					require.Len(t, ddlEvent.Objects, 0)
					return nil
				},
			},
			querier:     newNoTableQuerier(),
			wantObjects: 0,
			wantErr:     nil,
		},
		{
			name:      "error - processor fails",
			statement: `CREATE TABLE public.users (id integer);`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errTest
				},
			},
			querier: newNoTableQuerier(),
			wantErr: errTest,
		},
		{
			name:      "error - querier fails with non-ErrNoRows error",
			statement: `CREATE TABLE public.users (id integer);`,
			timestamp: "2024-01-01T00:00:00Z",
			processor: &mocks.Processor{ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error { return nil }},
			querier:   &pgmocks.Querier{QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error { return errTest }},
			wantErr:   errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			restore := newPGSnapshotWALRestore(tc.processor, tc.querier)

			err := restore.processDDLStatement(context.Background(), tc.statement, tc.timestamp)

			if tc.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint(1), tc.processor.GetProcessCalls())
			}
		})
	}
}

func TestPGSnapshotWALRestore_getTableObject(t *testing.T) {
	t.Parallel()

	errTest := errors.New("test error")

	tests := []struct {
		name    string
		schema  string
		table   string
		querier *pgmocks.Querier

		wantObjType string
		wantErr     error
	}{
		{
			name:   "ok - table found",
			schema: "public",
			table:  "users",
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					obj := &wal.DDLObject{
						Type:     "table",
						Identity: "public.users",
						Schema:   "public",
						OID:      "12345",
					}
					jsonBytes, err := jsonlib.Marshal(obj)
					if err != nil {
						return err
					}
					if len(dest) > 0 {
						if jsonDest, ok := dest[0].(*[]byte); ok {
							*jsonDest = jsonBytes
						}
					}
					return nil
				},
			},
			wantObjType: "table",
			wantErr:     nil,
		},
		{
			name:   "error - table not found",
			schema: "public",
			table:  "nonexistent",
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return pglib.ErrNoRows
				},
			},
			wantErr: pglib.ErrNoRows,
		},
		{
			name:   "error - query fails",
			schema: "public",
			table:  "users",
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			restore := newPGSnapshotWALRestore(nil, tc.querier)

			obj, err := restore.getTableObject(context.Background(), tc.schema, tc.table)

			if tc.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, obj)
				require.Equal(t, tc.wantObjType, obj.Type)
			}
		})
	}
}

func TestIsDDLStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		want      bool
	}{
		{
			name:      "CREATE TABLE",
			statement: "CREATE TABLE users (id integer);",
			want:      true,
		},
		{
			name:      "CREATE TABLE lowercase",
			statement: "create table users (id integer);",
			want:      true,
		},
		{
			name:      "ALTER TABLE",
			statement: "ALTER TABLE users ADD COLUMN email text;",
			want:      true,
		},
		{
			name:      "DROP TABLE",
			statement: "DROP TABLE users;",
			want:      true,
		},
		{
			name:      "CREATE INDEX",
			statement: "CREATE INDEX idx_users ON users(id);",
			want:      true,
		},
		{
			name:      "CREATE UNIQUE INDEX",
			statement: "CREATE UNIQUE INDEX idx_users ON users(id);",
			want:      true,
		},
		{
			name:      "CREATE SEQUENCE",
			statement: "CREATE SEQUENCE user_seq;",
			want:      true,
		},
		{
			name:      "DROP SEQUENCE",
			statement: "DROP SEQUENCE user_seq;",
			want:      true,
		},
		{
			name:      "CREATE SCHEMA",
			statement: "CREATE SCHEMA myschema;",
			want:      true,
		},
		{
			name:      "GRANT",
			statement: "GRANT SELECT ON users TO readonly;",
			want:      true,
		},
		{
			name:      "REVOKE",
			statement: "REVOKE SELECT ON users FROM readonly;",
			want:      true,
		},
		{
			name:      "COMMENT ON TABLE",
			statement: "COMMENT ON TABLE users IS 'User table';",
			want:      true,
		},
		{
			name:      "COMMENT ON COLUMN",
			statement: "COMMENT ON COLUMN users.name IS 'User name';",
			want:      true,
		},
		{
			name:      "INSERT - not DDL",
			statement: "INSERT INTO users VALUES (1, 'alice');",
			want:      false,
		},
		{
			name:      "SELECT - not DDL",
			statement: "SELECT * FROM users;",
			want:      false,
		},
		{
			name:      "UPDATE - not DDL",
			statement: "UPDATE users SET name = 'bob' WHERE id = 1;",
			want:      false,
		},
		{
			name:      "DELETE - not DDL",
			statement: "DELETE FROM users WHERE id = 1;",
			want:      false,
		},
		{
			name:      "empty statement",
			statement: "",
			want:      false,
		},
		{
			name:      "whitespace only",
			statement: "   ",
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isDDLStatement(tc.statement)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestExtractCommandTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name:      "CREATE TABLE",
			statement: "CREATE TABLE users (id integer);",
			want:      "CREATE TABLE",
		},
		{
			name:      "CREATE TABLE with IF NOT EXISTS",
			statement: "CREATE TABLE IF NOT EXISTS users (id integer);",
			want:      "CREATE TABLE",
		},
		{
			name:      "CREATE UNIQUE INDEX",
			statement: "CREATE UNIQUE INDEX idx_users ON users(id);",
			want:      "CREATE UNIQUE INDEX",
		},
		{
			name:      "CREATE INDEX",
			statement: "CREATE INDEX idx_users ON users(id);",
			want:      "CREATE INDEX",
		},
		{
			name:      "ALTER TABLE",
			statement: "ALTER TABLE users ADD COLUMN email text;",
			want:      "ALTER TABLE",
		},
		{
			name:      "DROP TABLE",
			statement: "DROP TABLE users;",
			want:      "DROP TABLE",
		},
		{
			name:      "COMMENT ON TABLE",
			statement: "COMMENT ON TABLE users IS 'User table';",
			want:      "COMMENT ON TABLE",
		},
		{
			name:      "COMMENT ON COLUMN",
			statement: "COMMENT ON COLUMN users.name IS 'User name';",
			want:      "COMMENT ON COLUMN",
		},
		{
			name:      "COMMENT ON (generic)",
			statement: "COMMENT ON EXTENSION pg_stat_statements IS 'track stats';",
			want:      "COMMENT ON",
		},
		{
			name:      "GRANT",
			statement: "GRANT SELECT ON users TO readonly;",
			want:      "GRANT",
		},
		{
			name:      "lowercase statement",
			statement: "create table users (id integer);",
			want:      "CREATE TABLE",
		},
		{
			name:      "mixed case statement",
			statement: "CrEaTe TaBlE users (id integer);",
			want:      "CREATE TABLE",
		},
		{
			name:      "single word",
			statement: "CHECKPOINT;",
			want:      "CHECKPOINT;", // Single word commands include the semicolon
		},
		{
			name:      "unknown command",
			statement: "SOMETHING WEIRD HERE;",
			want:      "SOMETHING WEIRD",
		},
		{
			name:      "empty statement",
			statement: "",
			want:      "UNKNOWN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractCommandTag(tc.statement)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestExtractSchemaAndObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		statement      string
		commandTag     string
		wantSchema     string
		wantObjects    []string
		wantObjectsLen int
	}{
		{
			name:        "CREATE TABLE with schema",
			statement:   "CREATE TABLE public.users (id integer);",
			commandTag:  "CREATE TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "CREATE TABLE without schema",
			statement:   "CREATE TABLE users (id integer);",
			commandTag:  "CREATE TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "CREATE TABLE with IF NOT EXISTS",
			statement:   "CREATE TABLE IF NOT EXISTS public.users (id integer);",
			commandTag:  "CREATE TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "ALTER TABLE with schema",
			statement:   "ALTER TABLE public.users ADD COLUMN email text;",
			commandTag:  "ALTER TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "ALTER TABLE without schema",
			statement:   "ALTER TABLE users ADD COLUMN email text;",
			commandTag:  "ALTER TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "DROP TABLE with schema",
			statement:   "DROP TABLE public.users;",
			commandTag:  "DROP TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "DROP TABLE IF EXISTS",
			statement:   "DROP TABLE IF EXISTS public.users;",
			commandTag:  "DROP TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:        "CREATE TABLE with custom schema",
			statement:   "CREATE TABLE myschema.users (id integer);",
			commandTag:  "CREATE TABLE",
			wantSchema:  "myschema",
			wantObjects: []string{"myschema.users"},
		},
		{
			name:        "CREATE TABLE with quoted names",
			statement:   `CREATE TABLE "public"."users" (id integer);`,
			commandTag:  "CREATE TABLE",
			wantSchema:  "public",
			wantObjects: []string{"public.users"},
		},
		{
			name:           "CREATE INDEX - no objects extracted",
			statement:      "CREATE INDEX idx_users ON users(id);",
			commandTag:     "CREATE INDEX",
			wantSchema:     "public",
			wantObjectsLen: 0,
		},
		{
			name:           "GRANT - no objects extracted",
			statement:      "GRANT SELECT ON users TO readonly;",
			commandTag:     "GRANT",
			wantSchema:     "public",
			wantObjectsLen: 0,
		},
		{
			name:           "unknown command",
			statement:      "SOMETHING UNKNOWN;",
			commandTag:     "SOMETHING UNKNOWN",
			wantSchema:     "public",
			wantObjectsLen: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotSchema, gotObjects := extractSchemaAndObjects(tc.statement, tc.commandTag)
			require.Equal(t, tc.wantSchema, gotSchema)

			if tc.wantObjects != nil {
				require.Equal(t, tc.wantObjects, gotObjects)
			} else {
				require.Len(t, gotObjects, tc.wantObjectsLen)
			}
		})
	}
}

func TestParseSchemaTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		qualifiedName string
		defaultSchema string
		wantSchema    string
		wantTable     string
	}{
		{
			name:          "schema.table format",
			qualifiedName: "public.users",
			defaultSchema: "myschema",
			wantSchema:    "public",
			wantTable:     "users",
		},
		{
			name:          "table only - uses default schema",
			qualifiedName: "users",
			defaultSchema: "myschema",
			wantSchema:    "myschema",
			wantTable:     "users",
		},
		{
			name:          "table only with public default",
			qualifiedName: "orders",
			defaultSchema: "public",
			wantSchema:    "public",
			wantTable:     "orders",
		},
		{
			name:          "custom schema qualified",
			qualifiedName: "finance.transactions",
			defaultSchema: "public",
			wantSchema:    "finance",
			wantTable:     "transactions",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotSchema, gotTable := parseSchemaTable(tc.qualifiedName, tc.defaultSchema)
			require.Equal(t, tc.wantSchema, gotSchema)
			require.Equal(t, tc.wantTable, gotTable)
		})
	}
}
