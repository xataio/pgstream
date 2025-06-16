// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	err1 = "error1"
	err2 = "error2"

	table1 = "table1"
	table2 = "table2"
)

var errTest = errors.New("oh noes")

func Test_NewErrors(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name string
		err  error

		wantErr Errors
	}{
		{
			name: "nil error",
			err:  nil,

			wantErr: nil,
		},
		{
			name: "normal error",
			err:  errTest,

			wantErr: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{errTest.Error()},
				},
			},
		},
		{
			name: "global error",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{errTest.Error()},
				},
			},

			wantErr: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{errTest.Error()},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotErr := NewErrors(testSchema, tc.err)
			require.Equal(t, gotErr, tc.wantErr)
		})
	}
}

func TestErrors_Error(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name string
		err  error

		wantStr string
	}{
		{
			name: "nil error",
			err:  (Errors)(nil),

			wantStr: "",
		},
		{
			name: "empty error",
			err:  Errors{},

			wantStr: "",
		},
		{
			name: "with one snapshot error",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1},
				},
			},

			wantStr: testSchema + ": " + err1,
		},
		{
			name: "with multiple snapshot errors",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1, err2},
				},
			},

			wantStr: fmt.Sprintf("%s: %s;%s", testSchema, err1, err2),
		},
		{
			name: "with one table error",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						table1: err1,
					},
				},
			},

			wantStr: fmt.Sprintf("%s: %s: %s", testSchema, table1, err1),
		},
		{
			name: "with multiple table errors",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema: testSchema,
					TableErrors: map[string]string{
						table1: err1,
						table2: err2,
					},
				},
			},

			wantStr: fmt.Sprintf("%s: %s: %s;%s: %s", testSchema, table1, err1, table2, err2),
		},
		{
			name: "with snapshot and table errors",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1, err2},
					TableErrors: map[string]string{
						table1: err1,
						table2: err2,
					},
				},
			},

			wantStr: fmt.Sprintf("%s: %s;%s;%s: %s;%s: %s", testSchema, err1, err2, table2, err2, table1, err1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			errMsg := tc.err.Error()
			if tc.wantStr != errMsg {
				// check the schema name
				require.True(t, strings.HasPrefix(errMsg, testSchema+": "), "error message expected: %q, got: %q", tc.wantStr, errMsg)

				// get the trimmed string without schema name to compare the errors
				errMsg = strings.TrimPrefix(errMsg, testSchema+": ")
				trimmedWantStr := strings.TrimPrefix(tc.wantStr, testSchema+": ")

				// Split the error messages by semicolon to compare parts
				// This allows for more flexible error message checking
				// especially when the order of errors might change.
				partsWanted := strings.Split(trimmedWantStr, ";")
				partsGot := strings.Split(errMsg, ";")
				require.ElementsMatch(t, partsWanted, partsGot, "error message expected: %q, got: %q", tc.wantStr, errMsg)
			}
		})
	}
}

func TestErrors_GetSchemaErrors(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name string
		err  Errors

		wantErrs *SchemaErrors
	}{
		{
			name: "ok",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1, err2},
				},
			},
			wantErrs: &SchemaErrors{
				Schema:       testSchema,
				GlobalErrors: []string{err1, err2},
			},
		},
		{
			name: "ok - no schema errors",
			err: Errors{
				"another-schema": &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1, err2},
				},
			},
			wantErrs: nil,
		},
		{
			name:     "ok - nil errors",
			err:      nil,
			wantErrs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			schemaErrs := tc.err.GetSchemaErrors(testSchema)
			require.Equal(t, tc.wantErrs, schemaErrs)
		})
	}
}

func TestErrors_AddError(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name   string
		err    Errors
		addErr error

		wantErr error
	}{
		{
			name:   "nil error",
			err:    nil,
			addErr: errTest,

			wantErr: (Errors)(nil),
		},
		{
			name:   "error",
			err:    Errors{},
			addErr: errTest,

			wantErr: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{errTest.Error()},
				},
			},
		},
		{
			name: "with global errors",
			err: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1, err2},
				},
			},
			addErr: errTest,

			wantErr: Errors{
				testSchema: &SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{err1, err2, errTest.Error()},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.err.AddError(testSchema, tc.addErr)
			require.Equal(t, tc.wantErr, tc.err)
		})
	}
}

func Test_NewSchemaErrors(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	otherSchema := "other-schema"

	tests := []struct {
		name     string
		schema   string
		err      error
		wantErrs *SchemaErrors
	}{
		{
			name:     "nil error",
			schema:   testSchema,
			err:      nil,
			wantErrs: nil,
		},
		{
			name:   "normal error",
			schema: testSchema,
			err:    errTest,
			wantErrs: &SchemaErrors{
				Schema:       testSchema,
				GlobalErrors: []string{errTest.Error()},
			},
		},
		{
			name:   "error is already SchemaErrors",
			schema: testSchema,
			err: &SchemaErrors{
				Schema:       otherSchema,
				GlobalErrors: []string{"err1", "err2"},
			},
			wantErrs: &SchemaErrors{
				Schema:       otherSchema,
				GlobalErrors: []string{"err1", "err2"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := NewSchemaErrors(tc.schema, tc.err)
			require.Equal(t, tc.wantErrs, got)
		})
	}
}

func TestSchemaErrors_IsTableError(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name  string
		table string
		err   *SchemaErrors

		wantTableErr bool
	}{
		{
			name:  "nil error",
			err:   nil,
			table: table1,

			wantTableErr: false,
		},
		{
			name:  "no errors",
			err:   &SchemaErrors{},
			table: table1,

			wantTableErr: false,
		},
		{
			name: "with snapshto errors",
			err: &SchemaErrors{
				Schema:       testSchema,
				GlobalErrors: []string{err1},
			},
			table: table1,

			wantTableErr: true,
		},
		{
			name: "with table errors",
			err: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: err1,
				},
			},
			table: table1,

			wantTableErr: true,
		},
		{
			name: "wildcard table",
			err: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: err1,
				},
			},
			table: "*",

			wantTableErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.err.IsTableError(tc.table)
			require.Equal(t, tc.wantTableErr, got)
		})
	}
}

func TestSchemaErrors_GetFailedTables(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name string
		err  *SchemaErrors

		wantTables []string
	}{
		{
			name: "nil error",
			err:  nil,

			wantTables: nil,
		},
		{
			name: "no failed tables",
			err:  &SchemaErrors{},

			wantTables: []string{},
		},
		{
			name: "with failed tables",
			err: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: err1,
					table2: err2,
				},
			},

			wantTables: []string{table1, table2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tables := tc.err.GetFailedTables()
			require.ElementsMatch(t, tc.wantTables, tables)
		})
	}
}

func TestSchemaErrors_AddTableError(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name   string
		err    *SchemaErrors
		table  string
		addErr error

		wantErrs *SchemaErrors
	}{
		{
			name:   "nil schema errors",
			err:    nil,
			table:  table1,
			addErr: errTest,

			wantErrs: nil,
		},
		{
			name:   "nil add error",
			err:    &SchemaErrors{},
			table:  table1,
			addErr: nil,

			wantErrs: &SchemaErrors{},
		},
		{
			name: "new table error",
			err: &SchemaErrors{
				Schema: testSchema,
			},
			table:  table1,
			addErr: errTest,

			wantErrs: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: errTest.Error(),
				},
			},
		},
		{
			name: "existing table errors",
			err: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: errTest.Error(),
				},
			},
			table:  table2,
			addErr: errTest,

			wantErrs: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: errTest.Error(),
					table2: errTest.Error(),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.err.AddTableError(tc.table, tc.addErr)
			require.Equal(t, tc.wantErrs, tc.err)
		})
	}
}

func TestSchemaErrors_IsGlobalError(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"

	tests := []struct {
		name string
		err  *SchemaErrors

		wantIsGlobalErr bool
	}{
		{
			name: "nil error",
			err:  nil,

			wantIsGlobalErr: false,
		},
		{
			name: "with snapshot error",
			err: &SchemaErrors{
				Schema:       testSchema,
				GlobalErrors: []string{err1, err2},
			},

			wantIsGlobalErr: true,
		},
		{
			name: "with table error",
			err: &SchemaErrors{
				Schema: testSchema,
				TableErrors: map[string]string{
					table1: err1,
				},
			},

			wantIsGlobalErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.err.IsGlobalError()
			require.Equal(t, tc.wantIsGlobalErr, got)
		})
	}
}
