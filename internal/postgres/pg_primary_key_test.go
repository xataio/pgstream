// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

func Test_GetPrimaryKeyColumns(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")
	testSchema := "test_schema"
	testTable := "test_table"

	// scanColumns builds a ScanFn that emits the provided column names in order,
	// one per Next()/Scan() call.
	scanColumns := func(cols ...string) func(i uint, dest ...any) error {
		return func(i uint, dest ...any) error {
			require.Len(t, dest, 1)
			colName, ok := dest[0].(*string)
			require.True(t, ok)
			*colName = cols[i-1]
			return nil
		}
	}

	tests := []struct {
		name    string
		querier *pgmocks.Querier

		wantColumns []string
		wantErr     error
	}{
		{
			name: "ok - single primary key",
			querier: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, pglib.PrimaryKeyColumnsQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i <= 1 },
						ScanFn:  scanColumns("id"),
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			wantColumns: []string{"id"},
			wantErr:     nil,
		},
		{
			name: "ok - composite primary key in order",
			querier: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, pglib.PrimaryKeyColumnsQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i <= 2 },
						ScanFn:  scanColumns("tenant_id", "id"),
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			wantColumns: []string{"tenant_id", "id"},
			wantErr:     nil,
		},
		{
			name: "ok - no primary key returns empty slice",
			querier: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, pglib.PrimaryKeyColumnsQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return false },
						ScanFn:  func(i uint, dest ...any) error { return errTest },
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			wantColumns: []string{},
			wantErr:     nil,
		},
		{
			name: "error - querying columns",
			querier: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},
			wantColumns: nil,
			wantErr:     errTest,
		},
		{
			name: "error - scanning column",
			querier: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i <= 1 },
						ScanFn:  func(i uint, dest ...any) error { return errTest },
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			wantColumns: nil,
			wantErr:     errTest,
		},
		{
			name: "error - rows error",
			querier: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return false },
						ScanFn:  func(i uint, dest ...any) error { return nil },
						ErrFn:   func() error { return errTest },
					}, nil
				},
			},
			wantColumns: nil,
			wantErr:     errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			columns, err := pglib.GetPrimaryKeyColumns(context.Background(), tc.querier, testSchema, testTable)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantColumns, columns)
		})
	}
}
