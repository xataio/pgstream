// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/stream"
)

const (
	oidInt4    = 23 // resolvable via the pgx static type map (no DB lookup)
	oidText    = 25
	oidUnknown = 999999 // not in the static map; forces a pg_type lookup
)

func TestSchemaTypeCompatibilityCheck_Run_AllTypesResolvable(t *testing.T) {
	t.Parallel()

	check := &SchemaTypeCompatibilityCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "orders", Column: "id", TypeOID: oidInt4},
			{Schema: "public", Table: "orders", Column: "name", TypeOID: oidText},
		}, nil),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestSchemaTypeCompatibilityCheck_Run_UnresolvableTypeReturnsFinding(t *testing.T) {
	t.Parallel()

	check := &SchemaTypeCompatibilityCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "orders", Column: "id", TypeOID: oidInt4},
			{Schema: "public", Table: "orders", Column: "weird", TypeOID: oidUnknown},
		}, errors.New("no rows")),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, "column public.orders.weird")
	require.Contains(t, findings[0].Message, "unsupported type OID 999999")
}

func TestSchemaTypeCompatibilityCheck_Run_FiltersTablesByScope(t *testing.T) {
	t.Parallel()

	sel, err := stream.NewTableSelection([]string{"public.*"}, []string{"public.audit_log"})
	require.NoError(t, err)

	check := &SchemaTypeCompatibilityCheck{
		Selection: sel,
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "billing", Table: "invoices", Column: "c", TypeOID: oidUnknown},
			{Schema: "public", Table: "audit_log", Column: "c", TypeOID: oidUnknown},
			{Schema: "public", Table: "orders", Column: "c", TypeOID: oidUnknown},
		}, errors.New("no rows")),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, "column public.orders.c")
}

func TestSchemaTypeCompatibilityCheck_Run_SourceAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, checkErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestSchemaTypeCompatibilityCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return nil, queryErr
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying source column types")
}

func TestSchemaTypeCompatibilityCheck_Run_ScanFails(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("scan failed")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return &mocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(uint, ...any) error { return scanErr },
						ErrFn:  func() error { return nil },
					}, nil
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, scanErr)
	require.ErrorContains(t, err, "scanning row")
}

func TestSchemaTypeCompatibilityCheck_Run_RowsErr(t *testing.T) {
	t.Parallel()

	rowsErr := errors.New("rows failed")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return &mocks.Rows{
						NextFn: func(uint) bool { return false },
						ScanFn: func(uint, ...any) error {
							t.Fatal("Scan should not be called")
							return nil
						},
						ErrFn: func() error { return rowsErr },
					}, nil
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, rowsErr)
	require.ErrorContains(t, err, "iterating rows")
}

func TestSchemaTypeCompatibilityCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "schema_type_compatibility", (&SchemaTypeCompatibilityCheck{}).Name())
}

func TestBuildSchemaChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *stream.Config
		wantChecks int
	}{
		{
			name: "no source postgres url returns no checks",
			cfg:  &stream.Config{},
		},
		{
			name: "source postgres url returns schema type compatibility check",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
			},
			wantChecks: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup := BuildSchemaChecks(tc.cfg)

			require.Len(t, checks, tc.wantChecks)
			if tc.wantChecks == 0 {
				require.Nil(t, cleanup)
				return
			}
			require.NotNil(t, cleanup)
			_, ok := checks[0].(*SchemaTypeCompatibilityCheck)
			require.True(t, ok)
		})
	}
}

func TestBuildChecks_SelectedSchemaOnly(t *testing.T) {
	t.Parallel()

	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: "postgres://source",
			},
		},
	}

	checks, cleanup := BuildChecks(cfg, []Category{CategorySchema})

	require.NotNil(t, cleanup)
	require.Len(t, checks, 1)
	require.Equal(t, "schema_type_compatibility", checks[0].Name())
}

// sourceWithColumns returns an AcquireFunc whose Querier serves the given column
// rows from Query and answers the mapper's pg_type lookups from QueryRow. A
// non-nil queryRowErr makes every type lookup fail, simulating an unresolvable
// OID; standard OIDs are resolved by the pgx static map and never reach it.
func sourceWithColumns(t *testing.T, rows []schemaColumnRow, queryRowErr error) postgres.AcquireFunc {
	t.Helper()
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return schemaColumnRows(t, rows), nil
			},
			QueryRowFn: func(_ context.Context, dest []any, _ string, _ ...any) error {
				if queryRowErr != nil {
					return queryRowErr
				}
				t.Fatal("unexpected pg_type lookup")
				return nil
			},
		}, nil
	}
}

func schemaColumnRows(t *testing.T, rows []schemaColumnRow) postgres.Rows {
	t.Helper()
	return &mocks.Rows{
		NextFn: func(i uint) bool {
			return int(i) <= len(rows)
		},
		ScanFn: func(i uint, dest ...any) error {
			require.Len(t, dest, 4)
			row := rows[i-1]
			schema, ok := dest[0].(*string)
			require.True(t, ok)
			table, ok := dest[1].(*string)
			require.True(t, ok)
			column, ok := dest[2].(*string)
			require.True(t, ok)
			typeOID, ok := dest[3].(*int64)
			require.True(t, ok)
			*schema = row.Schema
			*table = row.Table
			*column = row.Column
			*typeOID = row.TypeOID
			return nil
		},
		ErrFn: func() error { return nil },
	}
}
