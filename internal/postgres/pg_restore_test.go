// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				criticalErrs: []error{
					errors.New("psql: error: FATAL:  database \"test\" does not exist"),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parsePgRestoreOutputErrs([]byte(tt.output))
			require.Equal(t, tt.wantErrs, err)
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
			name:    "generic error",
			line:    "pg_restore: error: connection failed",
			wantErr: errors.New("pg_restore: error: connection failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseErrorLine(tt.line)
			require.NotNil(t, err)
			require.ErrorAs(t, err, &tt.wantErr)
		})
	}
}
