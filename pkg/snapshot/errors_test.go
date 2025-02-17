// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"errors"
	"fmt"
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

	tests := []struct {
		name string
		err  error

		wantErr *Errors
	}{
		{
			name: "nil error",
			err:  nil,

			wantErr: nil,
		},
		{
			name: "normal error",
			err:  errTest,

			wantErr: &Errors{
				SnapshotErrMsgs: []string{errTest.Error()},
			},
		},
		{
			name: "snapshot error",
			err: &Errors{
				SnapshotErrMsgs: []string{"test"},
			},

			wantErr: &Errors{
				SnapshotErrMsgs: []string{"test"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotErr := NewErrors(tc.err)
			require.Equal(t, gotErr, tc.wantErr)
		})
	}
}

func TestErrors_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error

		wantStr string
	}{
		{
			name: "nil error",
			err:  (*Errors)(nil),

			wantStr: "",
		},
		{
			name: "empty error",
			err:  &Errors{},

			wantStr: "",
		},
		{
			name: "with one snapshot error",
			err: &Errors{
				SnapshotErrMsgs: []string{err1},
			},

			wantStr: err1,
		},
		{
			name: "with multiple snapshot errors",
			err: &Errors{
				SnapshotErrMsgs: []string{err1, err2},
			},

			wantStr: fmt.Sprintf("%s;%s", err1, err2),
		},
		{
			name: "with one table error",
			err: &Errors{
				Tables: []TableError{
					{ErrorMsg: err1, Table: table1},
				},
			},

			wantStr: fmt.Sprintf("%s: %s", table1, err1),
		},
		{
			name: "with multiple table errors",
			err: &Errors{
				Tables: []TableError{
					{ErrorMsg: err1, Table: table1},
					{ErrorMsg: err2, Table: table2},
				},
			},

			wantStr: fmt.Sprintf("%s: %s;%s: %s", table1, err1, table2, err2),
		},
		{
			name: "with snapshot and table errors",
			err: &Errors{
				SnapshotErrMsgs: []string{err1, err2},
				Tables: []TableError{
					NewTableError(table1, errors.New(err1)),
					NewTableError(table2, errors.New(err2)),
				},
			},

			wantStr: fmt.Sprintf("%s;%s;%s: %s;%s: %s", err1, err2, table1, err1, table2, err2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			errMsg := tc.err.Error()
			require.Equal(t, tc.wantStr, errMsg)
		})
	}
}

func TestErrors_AddSnapshotErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    *Errors
		addErr error

		wantErr error
	}{
		{
			name:   "nil error",
			err:    nil,
			addErr: errTest,

			wantErr: (*Errors)(nil),
		},
		{
			name:   "error",
			err:    &Errors{},
			addErr: errTest,

			wantErr: &Errors{
				SnapshotErrMsgs: []string{errTest.Error()},
			},
		},
		{
			name: "with snapshot errors",
			err: &Errors{
				SnapshotErrMsgs: []string{err1, err2},
			},
			addErr: errTest,

			wantErr: &Errors{
				SnapshotErrMsgs: []string{err1, err2, errTest.Error()},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.err.AddSnapshotError(tc.addErr)
			require.Equal(t, tc.wantErr, tc.err)
		})
	}
}

func TestErrors_IsTableError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
		err   *Errors

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
			err:   &Errors{},
			table: table1,

			wantTableErr: false,
		},
		{
			name: "with snapshto errors",
			err: &Errors{
				SnapshotErrMsgs: []string{err1},
			},
			table: table1,

			wantTableErr: true,
		},
		{
			name: "with table errors",
			err: &Errors{
				Tables: []TableError{
					{Table: table1, ErrorMsg: err1},
				},
			},
			table: table1,

			wantTableErr: true,
		},
		{
			name: "wildcard table",
			err: &Errors{
				Tables: []TableError{
					{Table: table1, ErrorMsg: err1},
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

func TestErrors_GetFailedTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *Errors

		wantTables []string
	}{
		{
			name: "nil error",
			err:  nil,

			wantTables: nil,
		},
		{
			name: "no failed tables",
			err:  &Errors{},

			wantTables: []string{},
		},
		{
			name: "with failed tables",
			err: &Errors{
				Tables: []TableError{
					{Table: table1, ErrorMsg: err1},
					{Table: table2, ErrorMsg: err2},
				},
			},

			wantTables: []string{table1, table2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tables := tc.err.GetFailedTables()
			require.Equal(t, tc.wantTables, tables)
		})
	}
}

func TestErrors_IsSnapshotError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *Errors

		wantIsSnapshotErr bool
	}{
		{
			name: "nil error",
			err:  nil,

			wantIsSnapshotErr: false,
		},
		{
			name: "with snapshot error",
			err: &Errors{
				SnapshotErrMsgs: []string{err1, err2},
			},

			wantIsSnapshotErr: true,
		},
		{
			name: "with table error",
			err: &Errors{
				Tables: []TableError{
					NewTableError(table1, errTest),
				},
			},

			wantIsSnapshotErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.err.IsSnapshotError()
			require.Equal(t, tc.wantIsSnapshotErr, got)
		})
	}
}
