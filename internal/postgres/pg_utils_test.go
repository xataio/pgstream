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
