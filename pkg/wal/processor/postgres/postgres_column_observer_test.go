// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
)

func TestPGColumnObserver_getGeneratedColumnNames(t *testing.T) {
	t.Parallel()

	quotedQualifiedTableName := `"test_schema"."test_table"`
	idColumn := "id"

	tests := []struct {
		name         string
		tableColumns map[string][]string
		pgConn       pglib.Querier

		wantColumns      []string
		wantTableColumns map[string][]string
		wantErr          error
	}{
		{
			name:         "ok - empty map",
			tableColumns: map[string][]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, generatedTableColumnsQuery, query)
					require.Equal(t, []any{testTable, testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(args ...any) error {
							require.Len(t, args, 1)
							colName, ok := args[0].(*string)
							require.True(t, ok, fmt.Sprintf("column name, expected *string, got %T", args[0]))
							*colName = "id"
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantColumns: []string{idColumn},
			wantTableColumns: map[string][]string{
				quotedQualifiedTableName: {idColumn},
			},
			wantErr: nil,
		},
		{
			name: "ok - existing table",
			tableColumns: map[string][]string{
				quotedQualifiedTableName: {idColumn},
			},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					return nil, errors.New("unexpected call to QueryFn")
				},
			},

			wantColumns: []string{idColumn},
			wantTableColumns: map[string][]string{
				quotedQualifiedTableName: {idColumn},
			},
			wantErr: nil,
		},
		{
			name:         "error - quering table columns",
			tableColumns: map[string][]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantColumns:      nil,
			wantTableColumns: map[string][]string{},
			wantErr:          errTest,
		},
		{
			name:         "error - scanning table column",
			tableColumns: map[string][]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, generatedTableColumnsQuery, query)
					require.Equal(t, []any{testTable, testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(args ...any) error {
							return errTest
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantColumns:      nil,
			wantTableColumns: map[string][]string{},
			wantErr:          errTest,
		},
		{
			name:         "error - rows error",
			tableColumns: map[string][]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, generatedTableColumnsQuery, query)
					require.Equal(t, []any{testTable, testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(args ...any) error {
							return nil
						},
						ErrFn: func() error { return errTest },
					}, nil
				},
			},

			wantColumns:      nil,
			wantTableColumns: map[string][]string{},
			wantErr:          errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := &pgColumnObserver{
				pgConn:                tc.pgConn,
				generatedTableColumns: synclib.NewStringMapFromMap(tc.tableColumns),
			}

			colNames, err := o.getGeneratedColumnNames(context.TODO(), "test_schema", "test_table")
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantColumns, colNames)
			require.Equal(t, tc.wantTableColumns, o.generatedTableColumns.GetMap())
		})
	}
}
