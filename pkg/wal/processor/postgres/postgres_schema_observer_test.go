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
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
)

func TestPGSchemaObserver_getGeneratedColumnNames(t *testing.T) {
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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, generatedTableColumnsQuery, query)
					require.Equal(t, []any{testTable, testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(_ uint, dest ...any) error {
							require.Len(t, dest, 1)
							colName, ok := dest[0].(*string)
							require.True(t, ok, fmt.Sprintf("column name, expected *string, got %T", dest[0]))
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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
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
			name:         "error - querying table columns",
			tableColumns: map[string][]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, generatedTableColumnsQuery, query)
					require.Equal(t, []any{testTable, testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, generatedTableColumnsQuery, query)
					require.Equal(t, []any{testTable, testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
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

			o := &pgSchemaObserver{
				pgConn:                tc.pgConn,
				generatedTableColumns: synclib.NewMapFromMap(tc.tableColumns),
				logger:                loglib.NewNoopLogger(),
			}

			colNames, err := o.getGeneratedColumnNames(context.TODO(), "test_schema", "test_table")
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantColumns, colNames)
			require.Equal(t, tc.wantTableColumns, o.generatedTableColumns.GetMap())
		})
	}
}

func TestPGSchemaObserver_isMaterializedView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		schema            string
		table             string
		materializedViews map[string]map[string]struct{}
		pgConn            pglib.Querier

		wantMaterialized bool
	}{
		{
			name:              "no existing materialized views - query postgres no views",
			schema:            testSchema,
			table:             testTable,
			materializedViews: map[string]map[string]struct{}{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, materializedViewsQuery, query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantMaterialized: false,
		},
		{
			name:              "no existing materialized views - query postgres with views",
			schema:            testSchema,
			table:             "mv_test",
			materializedViews: map[string]map[string]struct{}{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, materializedViewsQuery, query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 1)
							name, ok := dest[0].(*string)
							require.True(t, ok, fmt.Sprintf("materialized view name, expected *string, got %T", dest[0]))
							*name = "mv_test"
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantMaterialized: true,
		},
		{
			name:   "existing materialized views",
			schema: testSchema,
			table:  testTable,
			materializedViews: map[string]map[string]struct{}{
				`"test_schema"`: {`"test_table"`: {}},
			},

			wantMaterialized: true,
		},
		{
			name:              "no existing materialized views - error querying postgres",
			schema:            testSchema,
			table:             testTable,
			materializedViews: map[string]map[string]struct{}{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantMaterialized: false,
		},
		{
			name:              "no existing materialized views - error scanning results",
			schema:            testSchema,
			table:             testTable,
			materializedViews: map[string]map[string]struct{}{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, materializedViewsQuery, query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantMaterialized: false,
		},
		{
			name:              "no existing materialized views - error rows",
			schema:            testSchema,
			table:             testTable,
			materializedViews: map[string]map[string]struct{}{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, materializedViewsQuery, query)
					require.Equal(t, []any{testSchema}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return nil
						},
						ErrFn: func() error { return errTest },
					}, nil
				},
			},

			wantMaterialized: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := &pgSchemaObserver{
				materializedViews: synclib.NewMapFromMap(tc.materializedViews),
				pgConn:            tc.pgConn,
				logger:            loglib.NewNoopLogger(),
			}

			isMaterialized := o.isMaterializedView(context.Background(), tc.schema, tc.table)
			require.Equal(t, tc.wantMaterialized, isMaterialized)
		})
	}
}

func TestPGSchemaObserver_updateMaterializedViews(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		logEntry          *schemalog.LogEntry
		materializedViews map[string]map[string]struct{}

		wantMaterializedViews map[string]map[string]struct{}
	}{
		{
			name: "no materialized views",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema:     schemalog.Schema{},
			},
			materializedViews: map[string]map[string]struct{}{},

			wantMaterializedViews: map[string]map[string]struct{}{
				`"test_schema"`: {},
			},
		},
		{
			name: "with materialized views",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema: schemalog.Schema{
					MaterializedViews: []schemalog.MaterializedView{
						{Name: "mv_1"},
						{Name: "mv_2"},
					},
				},
			},
			materializedViews: map[string]map[string]struct{}{},

			wantMaterializedViews: map[string]map[string]struct{}{
				`"test_schema"`: {`"mv_1"`: {}, `"mv_2"`: {}},
			},
		},
		{
			name: "with materialized views and existing schema",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema: schemalog.Schema{
					MaterializedViews: []schemalog.MaterializedView{
						{Name: "mv_1"},
						{Name: "mv_2"},
					},
				},
			},
			materializedViews: map[string]map[string]struct{}{
				`"test_schema"`: {`"mv_existing"`: {}},
			},

			wantMaterializedViews: map[string]map[string]struct{}{
				`"test_schema"`: {`"mv_1"`: {}, `"mv_2"`: {}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			obs := &pgSchemaObserver{
				materializedViews: synclib.NewMapFromMap(tc.materializedViews),
				logger:            loglib.NewNoopLogger(),
			}

			obs.updateMaterializedViews(tc.logEntry)
			require.Equal(t, tc.wantMaterializedViews, obs.materializedViews.GetMap())
		})
	}
}
