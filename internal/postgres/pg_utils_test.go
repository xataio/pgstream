// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func Test_NewQualifiedName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantQN  *QualifiedName
		wantErr error
	}{
		{
			name:    "single name",
			input:   "table",
			wantQN:  &QualifiedName{name: "table"},
			wantErr: nil,
		},
		{
			name:    "qualified name",
			input:   "schema.table",
			wantQN:  &QualifiedName{schema: "schema", name: "table"},
			wantErr: nil,
		},
		{
			name:    "invalid qualified name",
			input:   "a.b.c",
			wantQN:  nil,
			wantErr: errUnexpectedQualifiedName,
		},
		{
			name:    "empty string",
			input:   "",
			wantQN:  &QualifiedName{name: ""},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qn, err := NewQualifiedName(tc.input)
			require.Equal(t, tc.wantErr, err)
			require.Equal(t, tc.wantQN, qn)
		})
	}
}

func Test_QualifiedName_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		qn       QualifiedName
		expected string
	}{
		{
			name:     "no schema",
			qn:       QualifiedName{name: "table"},
			expected: "table",
		},
		{
			name:     "with schema",
			qn:       QualifiedName{schema: "schema", name: "table"},
			expected: `"schema"."table"`,
		},
		{
			name:     "empty name",
			qn:       QualifiedName{schema: "schema", name: ""},
			expected: `"schema".""`,
		},
		{
			name:     "qualified name",
			qn:       QualifiedName{schema: "schema", name: `"Table"`},
			expected: `"schema"."Table"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.qn.String()
			require.Equal(t, tc.expected, got)
		})
	}
}

func Test_QualifiedName_Schema_Name(t *testing.T) {
	t.Parallel()

	qn := QualifiedName{schema: "myschema", name: "mytable"}
	require.Equal(t, "myschema", qn.Schema())
	require.Equal(t, "mytable", qn.Name())

	qn2 := QualifiedName{name: "onlytable"}
	require.Equal(t, "", qn2.Schema())
	require.Equal(t, "onlytable", qn2.Name())
}

func Test_QuoteIdentifier(t *testing.T) {
	t.Parallel()
	require.Equal(t, `"table"`, QuoteIdentifier("table"))
	require.Equal(t, `"schema"`, QuoteIdentifier("schema"))

	// Test with already quoted identifier
	require.Equal(t, `"quoted"`, QuoteIdentifier(`"quoted"`))
}

func Test_QuoteQualifiedIdentifier(t *testing.T) {
	t.Parallel()
	require.Equal(t, `"schema"."table"`, QuoteQualifiedIdentifier("schema", "table"))
	require.Equal(t, `"a"."b"`, QuoteQualifiedIdentifier("a", "b"))

	// Test with already quoted identifiers
	require.Equal(t, `"schema"."Table"`, QuoteQualifiedIdentifier(`"schema"`, `"Table"`))
}

func Test_newIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string

		wantIdentifier pgx.Identifier
		wantErr        error
	}{
		{
			name:      "ok - table name",
			tableName: "test_table",

			wantIdentifier: pgx.Identifier{"test_table"},
			wantErr:        nil,
		},
		{
			name:      "ok - qualified table name",
			tableName: "test_schema.test_table",

			wantIdentifier: pgx.Identifier{"test_schema", "test_table"},
			wantErr:        nil,
		},
		{
			name:      "ok - quoted qualified table name",
			tableName: `"test_schema"."test_table"`,

			wantIdentifier: pgx.Identifier{"test_schema", "test_table"},
			wantErr:        nil,
		},
		{
			name:      "error - invalid table name",
			tableName: "invalid.test.table",

			wantIdentifier: nil,
			wantErr:        errors.New("invalid table name: invalid.test.table"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id, err := newIdentifier(tc.tableName)
			require.Equal(t, tc.wantErr, err)
			require.Equal(t, tc.wantIdentifier, id)
		})
	}
}

func Test_IsQuotedIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "quoted identifier",
			input:    `"table"`,
			expected: true,
		},
		{
			name:     "unquoted identifier",
			input:    "table",
			expected: false,
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "only opening quote",
			input:    `"table`,
			expected: false,
		},
		{
			name:     "only closing quote",
			input:    `table"`,
			expected: false,
		},
		{
			name:     "single quote",
			input:    `"`,
			expected: false,
		},
		{
			name:     "two quotes",
			input:    `""`,
			expected: false,
		},
		{
			name:     "three characters with quotes",
			input:    `"a"`,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsQuotedIdentifier(tc.input)
			require.Equal(t, tc.expected, got)
		})
	}
}

func Test_escapeConnectionURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rawURL   string
		expected string
		wantErr  error
	}{
		{
			name:     "postgres url with special characters in password",
			rawURL:   "postgres://user:p^ssword@localhost:5432/mydb",
			expected: "postgres://user:p%5Essword@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with percent signs in password",
			rawURL:   "postgres://user:p%ss@localhost:5432/mydb",
			expected: "postgres://user:p%25ss@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with ampersand in password",
			rawURL:   "postgres://user:p&ss@localhost:5432/mydb",
			expected: "postgres://user:p%26ss@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with equals sign in password",
			rawURL:   "postgres://user:p=ss@localhost:5432/mydb",
			expected: "postgres://user:p%3Dss@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with question mark in password",
			rawURL:   "postgres://user:p?ss@localhost:5432/mydb",
			expected: "postgres://user:p%3Fss@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with forward slash in password",
			rawURL:   "postgres://user:p/ss@localhost:5432/mydb",
			expected: "postgres://user:p%2Fss@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "simple password without special characters",
			rawURL:   "postgres://user:password@localhost:5432/mydb",
			expected: "postgres://user:password@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "non-postgres url should return unchanged",
			rawURL:   "mysql://user:password@localhost:3306/mydb",
			expected: "mysql://user:password@localhost:3306/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with port and parameters",
			rawURL:   "postgres://user:p^ss@localhost:5432/mydb?sslmode=disable",
			expected: "postgres://user:p%5Ess@localhost:5432/mydb?sslmode=disable",
			wantErr:  nil,
		},
		{
			name:     "url with password with colons",
			rawURL:   "postgres://user:password:with:colons:p^ss@localhost:5432/mydb",
			expected: "postgres://user:password%3Awith%3Acolons%3Ap%5Ess@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "url with no password",
			rawURL:   "postgres://user@localhost:5432/mydb",
			expected: "postgres://user@localhost:5432/mydb",
			wantErr:  nil,
		},
		{
			name:     "invalid postgres url format",
			rawURL:   "postgres://invalid-format",
			expected: "",
			wantErr:  errInvalidURL,
		},
		{
			name:     "postgres url missing username",
			rawURL:   "postgres://:password@localhost:5432/mydb",
			expected: "",
			wantErr:  errInvalidURL,
		},
		{
			name:     "postgres url missing host",
			rawURL:   "postgres://user:password@",
			expected: "",
			wantErr:  errInvalidURL,
		},
		{
			name:     "postgres url escaped password",
			rawURL:   "postgres://user:p%5Essword@localhost:5432/mydb",
			expected: "postgres://user:p%5Essword@localhost:5432/mydb",
			wantErr:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := escapeConnectionURL(tc.rawURL)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.expected, got)
		})
	}
}

func Test_IsValidReplicationSlotName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		slotName string
		wantErr  error
	}{
		{
			name:     "valid slot name with lowercase letters",
			slotName: "myslot",
			wantErr:  nil,
		},
		{
			name:     "valid slot name with numbers",
			slotName: "slot123",
			wantErr:  nil,
		},
		{
			name:     "valid slot name with underscores",
			slotName: "my_slot_name",
			wantErr:  nil,
		},
		{
			name:     "valid slot name with all allowed characters",
			slotName: "my_slot_123",
			wantErr:  nil,
		},
		{
			name:     "valid slot name starting with number",
			slotName: "123slot",
			wantErr:  nil,
		},
		{
			name:     "valid slot name starting with underscore",
			slotName: "_myslot",
			wantErr:  nil,
		},
		{
			name:     "invalid slot name with uppercase letters",
			slotName: "MySlot",
			wantErr:  errInvalidReplicationSlotName,
		},
		{
			name:     "invalid slot name with hyphen",
			slotName: "my-slot",
			wantErr:  errInvalidReplicationSlotName,
		},
		{
			name:     "invalid slot name with space",
			slotName: "my slot",
			wantErr:  errInvalidReplicationSlotName,
		},
		{
			name:     "invalid slot name with dot",
			slotName: "my.slot",
			wantErr:  errInvalidReplicationSlotName,
		},
		{
			name:     "invalid slot name with special characters",
			slotName: "my@slot",
			wantErr:  errInvalidReplicationSlotName,
		},
		{
			name:     "empty slot name",
			slotName: "",
			wantErr:  errInvalidReplicationSlotName,
		},
		{
			name:     "slot name with only numbers",
			slotName: "12345",
			wantErr:  nil,
		},
		{
			name:     "slot name with only underscores",
			slotName: "___",
			wantErr:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := IsValidReplicationSlotName(tc.slotName)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
