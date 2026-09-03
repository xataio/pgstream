// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveDatabaseFromConnectionString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connection string
		want       string
	}{
		{
			name:       "database is removed",
			connection: "postgres://pgstream:secret@localhost:5432/target_db?sslmode=disable",
			want:       "postgres://pgstream:secret@localhost:5432/?sslmode=disable",
		},
		{
			name:       "postgres database is preserved",
			connection: "postgres://pgstream:secret@localhost:5432/postgres?sslmode=disable",
			want:       "postgres://pgstream:secret@localhost:5432/postgres?sslmode=disable",
		},
		{
			name:       "user matching the database name is preserved",
			connection: "postgres://app:secret@app.internal:5432/app?sslmode=disable",
			want:       "postgres://app:secret@app.internal:5432/?sslmode=disable",
		},
		{
			name:       "host matching the database name is preserved",
			connection: "postgres://pgstream:secret@target_db:5432/target_db",
			want:       "postgres://pgstream:secret@target_db:5432/",
		},
		{
			name:       "no database to remove",
			connection: "postgres://pgstream:secret@localhost:5432/",
			want:       "postgres://pgstream:secret@localhost:5432/",
		},
		{
			name:       "dsn database is removed",
			connection: "host=localhost port=5432 user=app dbname=app sslmode=disable",
			want:       "host=localhost port=5432 user=app sslmode=disable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := RemoveDatabaseFromConnectionString(tc.connection)

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestPGRestoreOptionsToPGOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing string
		settings []string
		want     string
	}{
		{
			name: "session settings",
			settings: []string{
				"maintenance_work_mem=4GB",
				"max_parallel_maintenance_workers=4",
			},
			want: "-c maintenance_work_mem=4GB -c max_parallel_maintenance_workers=4",
		},
		{
			name:     "preserves existing options",
			existing: "-c search_path=public",
			settings: []string{"statement_timeout=0"},
			want:     "-c search_path=public -c statement_timeout=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := PGRestoreOptions{SessionSettings: tt.settings}
			require.Equal(t, tt.want, opts.toPGOptions(tt.existing))
		})
	}
}

func TestParsePgRestoreOutputErrs(t *testing.T) {
	tests := []struct {
		name             string
		output           string
		expectedErrs     int
		expectedIgnored  int
		expectedCritical int
		wantErrs         error
	}{
		{
			name:   "empty output",
			output: "",

			wantErrs: nil,
		},
		{
			name:   "no errors",
			output: "pg_restore: processing data for table \"users\"\npg_restore: finished\n",

			wantErrs: nil,
		},
		{
			name:   "single relation already exists error",
			output: "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationAlreadyExists{Details: "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists"},
				},
			},
		},
		{
			name:   "multiple primary keys error",
			output: "pg_restore: error: could not execute query: ERROR:  multiple primary keys for table \"users\" are not allowed\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationAlreadyExists{Details: "pg_restore: error: could not execute query: ERROR:  multiple primary keys for table \"users\" are not allowed"},
				},
			},
		},
		{
			name:   "constraint violation error",
			output: "psql: error: could not execute query: ERROR:  cannot drop schema public because other objects depend on it\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrConstraintViolation{Details: "psql: error: could not execute query: ERROR:  cannot drop schema public because other objects depend on it"},
				},
			},
		},
		{
			name:   "permission denied error",
			output: "pg_restore: error: could not execute query: ERROR:  permission denied to grant privileges as role \"admin\"\n",
			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrPermissionDenied{Details: "pg_restore: error: could not execute query: ERROR:  permission denied to grant privileges as role \"admin\""},
				},
			},
		},
		{
			name:   "critical error",
			output: "pg_restore: error: connection failed\n",

			wantErrs: &PGRestoreErrors{
				criticalErrs: []error{
					errors.New("pg_restore: error: connection failed"),
				},
			},
		},
		{
			name:   "error with detail line",
			output: "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists\nDETAIL:  Table already exists in schema public\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: DETAIL:  Table already exists in schema public", &ErrRelationAlreadyExists{Details: "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists"}),
				},
			},
		},
		{
			name: "multiple errors mixed types",
			output: `pg_restore: error: could not execute query: ERROR:  relation "users" already exists
pg_restore: error: connection to database failed
pg_restore: error: could not execute query: ERROR:  permission denied to grant privileges as role "admin"`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationAlreadyExists{Details: `pg_restore: error: could not execute query: ERROR:  relation "users" already exists`},
					&ErrPermissionDenied{Details: `pg_restore: error: could not execute query: ERROR:  permission denied to grant privileges as role "admin"`},
				},
				criticalErrs: []error{
					errors.New("pg_restore: error: connection to database failed"),
				},
			},
		},
		{
			name:   "psql error format",
			output: "psql: error: FATAL:  database \"test\" does not exist\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationDoesNotExist{Details: "psql: error: FATAL:  database \"test\" does not exist"},
				},
			},
		},
		{
			name:   "relation does not exist error from trigger drop",
			output: "ERROR:  relation \"public.vendor_products\" does not exist\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationDoesNotExist{Details: "ERROR:  relation \"public.vendor_products\" does not exist"},
				},
			},
		},
		{
			name:   "partition already attached error",
			output: "ERROR:  \"linking_queue_000\" is already a partition\n",

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationAlreadyExists{Details: "ERROR:  \"linking_queue_000\" is already a partition"},
				},
			},
		},
		{
			name: "mixed success and error output",
			output: `pg_restore: processing data for table "users"
pg_restore: error: could not execute query: ERROR:  relation "posts" already exists
pg_restore: finished`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					&ErrRelationAlreadyExists{Details: `pg_restore: error: could not execute query: ERROR:  relation "posts" already exists`},
				},
			},
		},
		{
			name: "ownership error on comment statement is ignorable",
			output: `ERROR:  must be owner of schema public
STATEMENT:  COMMENT ON SCHEMA public IS 'standard public schema';`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: %s", &ErrCommentOwnership{Details: "ERROR:  must be owner of schema public"}, "STATEMENT:  COMMENT ON SCHEMA public IS 'standard public schema';"),
				},
			},
		},
		{
			name: "ownership error on comment statement from pg_restore is ignorable",
			output: `pg_restore: error: could not execute query: ERROR:  must be owner of extension plpgsql
Command was: COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: %s", &ErrCommentOwnership{Details: "pg_restore: error: could not execute query: ERROR:  must be owner of extension plpgsql"}, "Command was: COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';"),
				},
			},
		},
		{
			name: "ownership error on non-comment statement stays critical",
			output: `ERROR:  must be owner of table users
STATEMENT:  ALTER TABLE public.users OWNER TO admin;`,

			wantErrs: &PGRestoreErrors{
				criticalErrs: []error{
					fmt.Errorf("%w: %s", errors.New("ERROR:  must be owner of table users"), "STATEMENT:  ALTER TABLE public.users OWNER TO admin;"),
				},
			},
		},
		{
			name: "error with command was line from pg_restore",
			output: `pg_restore: error: could not execute query: ERROR:  relation "users" already exists
Command was: CREATE TABLE public.users (id integer);`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: %s", &ErrRelationAlreadyExists{Details: `pg_restore: error: could not execute query: ERROR:  relation "users" already exists`}, "Command was: CREATE TABLE public.users (id integer);"),
				},
			},
		},
		{
			name: "statement echo containing ERROR keyword is not a new error",
			output: `ERROR:  must be owner of table error_log
STATEMENT:  COMMENT ON TABLE error_log IS 'ERROR entries';`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: %s", &ErrCommentOwnership{Details: "ERROR:  must be owner of table error_log"}, "STATEMENT:  COMMENT ON TABLE error_log IS 'ERROR entries';"),
				},
			},
		},
		{
			name: "multi-line statement echo keeps first line only",
			output: `ERROR:  permission denied for schema public
LINE 1: CREATE TABLE public.t_multi(
                     ^
STATEMENT:  CREATE TABLE public.t_multi(
  id int,
  name text
);`,

			wantErrs: &PGRestoreErrors{
				criticalErrs: []error{
					fmt.Errorf("%w: %s", errors.New("ERROR:  permission denied for schema public"), "STATEMENT:  CREATE TABLE public.t_multi("),
				},
			},
		},
		{
			name:   "statement line without preceding error is ignored",
			output: "STATEMENT:  COMMENT ON SCHEMA public IS 'standard public schema';\n",

			wantErrs: nil,
		},
		{
			name: "multi-line comment echo containing ERROR text stays ignorable",
			output: `ERROR:  must be owner of schema public
STATEMENT:  COMMENT ON SCHEMA public IS 'status codes:
ERROR means failure
DETAIL: none';`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: %s", &ErrCommentOwnership{Details: "ERROR:  must be owner of schema public"}, "STATEMENT:  COMMENT ON SCHEMA public IS 'status codes:"),
				},
			},
		},
		{
			name: "error following a multi-line statement echo is still parsed",
			output: `ERROR:  must be owner of schema public
STATEMENT:  COMMENT ON SCHEMA public IS 'first
line';
ERROR:  relation "users" already exists
STATEMENT:  CREATE TABLE public.users (id integer);`,

			wantErrs: &PGRestoreErrors{
				ignoredErrs: []error{
					fmt.Errorf("%w: %s", &ErrCommentOwnership{Details: "ERROR:  must be owner of schema public"}, "STATEMENT:  COMMENT ON SCHEMA public IS 'first"),
					fmt.Errorf("%w: %s", &ErrRelationAlreadyExists{Details: `ERROR:  relation "users" already exists`}, "STATEMENT:  CREATE TABLE public.users (id integer);"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parsePgRestoreOutputErrs([]byte(tt.output))
			require.Equal(t, tt.wantErrs, err)
		})
	}
}

func TestBuildRestoreError(t *testing.T) {
	t.Parallel()

	execErr := errors.New("exit status 1")

	tests := []struct {
		name    string
		output  []byte
		execErr error

		wantNil         bool
		wantContain     string
		wantAlsoContain string
	}{
		{
			name:    "no error - success",
			output:  []byte("pg_restore: finished\n"),
			execErr: nil,
			wantNil: true,
		},
		{
			// the output is the only record of the cause when nothing in it
			// parses as a recognised error, so it must survive into the error
			name:            "exec error with no parseable output preserves the output",
			output:          []byte("some unexpected output\n"),
			execErr:         execErr,
			wantContain:     "exit status 1",
			wantAlsoContain: "some unexpected output",
		},
		{
			name:            "exec error with a lost connection preserves the cause",
			output:          []byte("server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n"),
			execErr:         errors.New("exit status 2"),
			wantContain:     "exit status 2",
			wantAlsoContain: "server closed the connection unexpectedly",
		},
		{
			name:            "exec error with empty output says so",
			output:          []byte{},
			execErr:         execErr,
			wantContain:     "exit status 1",
			wantAlsoContain: "no output captured",
		},
		{
			name:        "exec error with parseable ERROR lines",
			output:      []byte("pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists\n"),
			execErr:     execErr,
			wantContain: "already exists",
		},
		{
			name:        "no exec error but output contains ERROR",
			output:      []byte("ERROR:  relation \"users\" already exists\n"),
			execErr:     nil,
			wantContain: "already exists",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := buildRestoreError(tc.output, tc.execErr)
			if tc.wantNil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantContain)
			if tc.wantAlsoContain != "" {
				assert.Contains(t, err.Error(), tc.wantAlsoContain)
			}
			assert.NotContains(t, err.Error(), "%!w(<nil>)")
		})
	}
}

func TestTailOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		output   []byte
		maxBytes int

		want string
	}{
		{
			name:     "empty",
			output:   []byte{},
			maxBytes: 16,
			want:     "",
		},
		{
			name:     "whitespace only",
			output:   []byte("  \n\t "),
			maxBytes: 16,
			want:     "",
		},
		{
			name:     "shorter than the limit is returned trimmed",
			output:   []byte("  connection lost\n"),
			maxBytes: 16,
			want:     "connection lost",
		},
		{
			name:     "longer than the limit keeps the tail",
			output:   []byte("aaaaaaaaaabbbb"),
			maxBytes: 4,
			want:     "...bbbb",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, tailOutput(tc.output, tc.maxBytes))
		})
	}
}

func TestIsErrorLine(t *testing.T) {
	tests := []struct {
		line     string
		expected bool
	}{
		{"pg_restore: error: could not execute query", true},
		{"ERROR:  relation already exists", true},
		{"psql: error: connection failed", true},
		{"pg_restore: processing data for table", false},
		{"DETAIL:  some detail", false},
		{"INFO:  some info", false},
		{"'ERROR means failure';", false},
		{"comment text mentioning ERROR mid-line", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			result := isErrorLine(tt.line)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsDetailLine(t *testing.T) {
	tests := []struct {
		line     string
		expected bool
	}{
		{"DETAIL:  Table already exists", true},
		{"DETAIL: some detail info", true},
		{"ERROR:  relation already exists", false},
		{"pg_restore: processing data", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			result := isDetailLine(tt.line)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsStatementLine(t *testing.T) {
	tests := []struct {
		line     string
		expected bool
	}{
		{"STATEMENT:  COMMENT ON SCHEMA public IS 'standard public schema';", true},
		{"Command was: CREATE TABLE public.users (id integer);", true},
		{"    Command was: CREATE TABLE public.users (id integer);", true},
		{"ERROR:  must be owner of schema public", false},
		{"DETAIL:  some detail", false},
		{"  id int,", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			result := isStatementLine(tt.line)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsCommentStatement(t *testing.T) {
	tests := []struct {
		line     string
		expected bool
	}{
		{"STATEMENT:  COMMENT ON SCHEMA public IS 'standard public schema';", true},
		{"Command was: COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';", true},
		{"STATEMENT:  ALTER TABLE public.users OWNER TO admin;", false},
		{"STATEMENT:  CREATE TABLE public.comment_on (id int);", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			result := isCommentStatement(tt.line)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTruncateStatement(t *testing.T) {
	short := "STATEMENT:  COMMENT ON SCHEMA public IS 'standard public schema';"
	assert.Equal(t, short, truncateStatement(short))

	long := "STATEMENT:  CREATE VIEW public.v AS SELECT " + strings.Repeat("a", maxStatementLen)
	truncated := truncateStatement(long)
	assert.Len(t, truncated, maxStatementLen+len("..."))
	assert.True(t, strings.HasSuffix(truncated, "..."))

	// the two-byte 'é' straddles the cut point and must not be split
	multibyte := strings.Repeat("a", maxStatementLen-1) + "éllo wörld"
	truncated = truncateStatement(multibyte)
	assert.True(t, utf8.ValidString(truncated))
	assert.True(t, strings.HasSuffix(truncated, "..."))
}

func TestParseErrorLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr error
	}{
		{
			name:    "relation already exists",
			line:    "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists",
			wantErr: &ErrRelationAlreadyExists{Details: "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists"},
		},
		{
			name:    "multiple primary keys",
			line:    "pg_restore: error: could not execute query: ERROR:  multiple primary keys for table \"users\" are not allowed",
			wantErr: &ErrRelationAlreadyExists{Details: "pg_restore: error: could not execute query: ERROR:  multiple primary keys for table \"users\" are not allowed"},
		},
		{
			name:    "constraint violation",
			line:    "psql: error: could not execute query: ERROR:  cannot drop schema public because other objects depend on it",
			wantErr: &ErrConstraintViolation{Details: "psql: error: could not execute query: ERROR:  cannot drop schema public because other objects depend on it"},
		},
		{
			name:    "permission denied",
			line:    "pg_restore: error: could not execute query: ERROR:  permission denied to grant privileges as role \"admin\"",
			wantErr: &ErrPermissionDenied{Details: "pg_restore: error: could not execute query: ERROR:  permission denied to grant privileges as role \"admin\""},
		},
		{
			name:    "relation does not exist",
			line:    `ERROR:  relation "public.vendor_products" does not exist`,
			wantErr: &ErrRelationDoesNotExist{Details: `ERROR:  relation "public.vendor_products" does not exist`},
		},
		{
			name:    "already a partition",
			line:    `ERROR:  "linking_queue_000" is already a partition`,
			wantErr: &ErrRelationAlreadyExists{Details: `ERROR:  "linking_queue_000" is already a partition`},
		},
		{
			name:    "connection lost",
			line:    `psql: error: connection to server was lost`,
			wantErr: &ErrTransientFailure{Details: `psql: error: connection to server was lost`},
		},
		{
			name:    "server closed the connection",
			line:    "pg_restore: error: could not execute query: server closed the connection unexpectedly",
			wantErr: &ErrTransientFailure{Details: "pg_restore: error: could not execute query: server closed the connection unexpectedly"},
		},
		{
			name:    "lock timeout",
			line:    `ERROR:  canceling statement due to lock timeout`,
			wantErr: &ErrTransientFailure{Details: `ERROR:  canceling statement due to lock timeout`},
		},
		{
			name:    "deadlock",
			line:    `ERROR:  deadlock detected`,
			wantErr: &ErrTransientFailure{Details: `ERROR:  deadlock detected`},
		},
		{
			name:    "connection refused",
			line:    `psql: error: connection to server at "localhost" (::1), port 5432 failed: Connection refused`,
			wantErr: &ErrTransientFailure{Details: `psql: error: connection to server at "localhost" (::1), port 5432 failed: Connection refused`},
		},
		{
			// a connection failure that reports a permanent cause keeps the
			// classification of that cause, so it is not retried
			name:    "connection failure with a permanent cause",
			line:    `psql: error: connection to server at "localhost" (::1), port 5432 failed: FATAL:  database "test" does not exist`,
			wantErr: &ErrRelationDoesNotExist{Details: `psql: error: connection to server at "localhost" (::1), port 5432 failed: FATAL:  database "test" does not exist`},
		},
		{
			// retrying a statement that is simply too slow times out again
			name:    "statement timeout",
			line:    `ERROR:  canceling statement due to statement timeout`,
			wantErr: errors.New(`ERROR:  canceling statement due to statement timeout`),
		},
		{
			name:    "generic error",
			line:    "pg_restore: error: unrecognized data block type",
			wantErr: errors.New("pg_restore: error: unrecognized data block type"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseErrorLine(tt.line)
			require.NotNil(t, err)
			require.IsType(t, tt.wantErr, err)
			require.Equal(t, tt.wantErr.Error(), err.Error())
		})
	}
}

func TestPGRestoreErrors_classification(t *testing.T) {
	t.Parallel()

	errCritical := errors.New("oh noes")
	errIgnored := &ErrRelationAlreadyExists{Details: "relation exists"}
	errTransient := &ErrTransientFailure{Details: "connection to server was lost"}

	tests := []struct {
		name string
		errs []error

		wantCritical  bool
		wantRetryable bool
		wantIgnored   int
	}{
		{
			name: "no errors",
			errs: nil,
		},
		{
			name:        "ignored only",
			errs:        []error{errIgnored},
			wantIgnored: 1,
		},
		{
			name:          "transient only",
			errs:          []error{errTransient},
			wantCritical:  true,
			wantRetryable: true,
		},
		{
			name:          "transient alongside ignored",
			errs:          []error{errTransient, errIgnored},
			wantCritical:  true,
			wantRetryable: true,
			wantIgnored:   1,
		},
		{
			name:         "transient alongside critical",
			errs:         []error{errTransient, errCritical},
			wantCritical: true,
		},
		{
			name:         "critical only",
			errs:         []error{errCritical},
			wantCritical: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			errs := NewPGRestoreErrors(tt.errs...)
			require.Equal(t, len(tt.errs) > 0, errs.HasErrors())
			require.Equal(t, tt.wantCritical, errs.HasCriticalErrors())
			require.Equal(t, tt.wantRetryable, errs.IsRetryable())
			require.Len(t, errs.GetIgnoredErrors(), tt.wantIgnored)
		})
	}
}

func TestRedactSecrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string

		want string
	}{
		{
			name: "no secret",
			in:   `STATEMENT:  CREATE TABLE users (id int);`,
			want: `STATEMENT:  CREATE TABLE users (id int);`,
		},
		{
			name: "create role scram hash",
			in:   `CREATE ROLE app WITH LOGIN PASSWORD 'SCRAM-SHA-256$4096:abc$def:ghi';`,
			want: `CREATE ROLE app WITH LOGIN PASSWORD '[REDACTED]';`,
		},
		{
			name: "alter role md5 hash",
			in:   `ALTER ROLE app PASSWORD 'md5d41d8cd98f00b204e9800998ecf8427e';`,
			want: `ALTER ROLE app PASSWORD '[REDACTED]';`,
		},
		{
			name: "create user encrypted, lowercase keyword",
			in:   `create user bob encrypted password 'hunter2';`,
			want: `create user bob encrypted PASSWORD '[REDACTED]';`,
		},
		{
			name: "doubled quote inside the secret does not end the match early",
			in:   `CREATE ROLE app PASSWORD 'we''ird' SUPERUSER;`,
			want: `CREATE ROLE app PASSWORD '[REDACTED]' SUPERUSER;`,
		},
		{
			name: "multiple statements on separate lines",
			in:   "CREATE ROLE a PASSWORD 'x';\nCREATE ROLE b PASSWORD 'y';",
			want: "CREATE ROLE a PASSWORD '[REDACTED]';\nCREATE ROLE b PASSWORD '[REDACTED]';",
		},
		{
			name: "password keyword without a literal is untouched",
			in:   `ALTER ROLE app PASSWORD NULL;`,
			want: `ALTER ROLE app PASSWORD NULL;`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, redactSecrets(tc.in))
		})
	}
}

func TestTailOutput_redactsSecrets(t *testing.T) {
	t.Parallel()

	// the path that matters: a roles restore that dies mid-way echoes the
	// failing statement with no ERROR: prefix, so nothing parses and the raw
	// tail is what reaches the error
	out := []byte("server closed the connection unexpectedly\n" +
		"CREATE ROLE app WITH LOGIN PASSWORD 'SCRAM-SHA-256$4096:secret';\n")

	got := tailOutput(out, maxRestoreOutputBytes)

	require.NotContains(t, got, "SCRAM-SHA-256")
	require.NotContains(t, got, "secret")
	require.Contains(t, got, "PASSWORD '[REDACTED]'")
	// the diagnostic that made preserving the tail worthwhile survives
	require.Contains(t, got, "server closed the connection unexpectedly")
	require.Contains(t, got, "CREATE ROLE app")
}

func TestRedactSecrets_copyRowContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string

		want string
	}{
		{
			name: "copy row payload redacted, table and line kept",
			in:   `CONTEXT:  COPY users, line 42: "1	alice@example.com	555-0100"`,
			want: `CONTEXT:  COPY users, line 42: [REDACTED ROW]`,
		},
		{
			name: "qualified table name",
			in:   `CONTEXT:  COPY labs.patient_snapshot_us, line 9001: "secret"`,
			want: `CONTEXT:  COPY labs.patient_snapshot_us, line 9001: [REDACTED ROW]`,
		},
		{
			name: "copy context without a payload is untouched",
			in:   `CONTEXT:  COPY users, line 42`,
			want: `CONTEXT:  COPY users, line 42`,
		},
		{
			name: "unrelated context line is untouched",
			in:   `CONTEXT:  PL/pgSQL function f() line 3 at RAISE`,
			want: `CONTEXT:  PL/pgSQL function f() line 3 at RAISE`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, redactSecrets(tc.in))
		})
	}
}

func TestTailOutput_redactsSecretsStraddlingTheCut(t *testing.T) {
	t.Parallel()

	// Redaction has to run before the output is cut down. When a secret
	// straddles the cut, the prefix the pattern matches on sits in the
	// discarded head, so redacting the tail alone would leave the trailing
	// fragment of the hash in the error.
	tests := []struct {
		name       string
		output     string
		maxBytes   int
		mustNotHit string
	}{
		{
			name:       "password literal straddling the cut",
			output:     strings.Repeat("x", 200) + `CREATE ROLE app PASSWORD 'SCRAM-SHA-256$4096:TOPSECRETHASH';`,
			maxBytes:   24,
			mustNotHit: "TOPSECRETHASH",
		},
		{
			name:       "copy row payload straddling the cut",
			output:     strings.Repeat("x", 200) + `CONTEXT:  COPY users, line 42: "1	alice@example.com	555-0100"`,
			maxBytes:   24,
			mustNotHit: "alice@example.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tailOutput([]byte(tc.output), tc.maxBytes)
			require.NotContains(t, got, tc.mustNotHit)
		})
	}
}
