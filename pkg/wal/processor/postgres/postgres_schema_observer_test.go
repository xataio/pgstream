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
	idColumn := `"id"`

	tests := []struct {
		name         string
		tableColumns map[string]map[string]struct{}
		pgConn       pglib.Querier

		wantColumns      map[string]struct{}
		wantTableColumns map[string]map[string]struct{}
		wantErr          error
	}{
		{
			name:         "ok - empty map",
			tableColumns: map[string]map[string]struct{}{},
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

			wantColumns: map[string]struct{}{idColumn: {}},
			wantTableColumns: map[string]map[string]struct{}{
				quotedQualifiedTableName: {idColumn: {}},
			},
			wantErr: nil,
		},
		{
			name: "ok - existing table",
			tableColumns: map[string]map[string]struct{}{
				quotedQualifiedTableName: {idColumn: {}},
			},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errors.New("unexpected call to QueryFn")
				},
			},

			wantColumns: map[string]struct{}{idColumn: {}},
			wantTableColumns: map[string]map[string]struct{}{
				quotedQualifiedTableName: {idColumn: {}},
			},
			wantErr: nil,
		},
		{
			name:         "error - querying table columns",
			tableColumns: map[string]map[string]struct{}{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantColumns:      nil,
			wantTableColumns: map[string]map[string]struct{}{},
			wantErr:          errTest,
		},
		{
			name:         "error - scanning table column",
			tableColumns: map[string]map[string]struct{}{},
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
			wantTableColumns: map[string]map[string]struct{}{},
			wantErr:          errTest,
		},
		{
			name:         "error - rows error",
			tableColumns: map[string]map[string]struct{}{},
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
			wantTableColumns: map[string]map[string]struct{}{},
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

func TestPGSchemaObserver_getSequenceColumns(t *testing.T) {
	t.Parallel()

	quotedQualifiedTableName := `"test_schema"."test_table"`
	idColumn := `"id"`
	createdAtColumn := `"created_at"`
	idSequenceName := "id_seq"
	createdAtSequenceName := "created_at_seq"
	quoteQualifiedSequenceName := func(seq string) string {
		return pglib.QuoteQualifiedIdentifier("test_schema", seq)
	}

	tests := []struct {
		name                 string
		columnTableSequences map[string]map[string]string
		pgConn               pglib.Querier

		wantColumns              []string
		wantColumnTableSequences map[string]map[string]string
		wantErr                  error
	}{
		{
			name:                 "ok - empty map",
			columnTableSequences: map[string]map[string]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(_ uint, dest ...any) error {
							require.Len(t, dest, 2)
							colName, ok := dest[0].(*string)
							require.True(t, ok, fmt.Sprintf("column name, expected *string, got %T", dest[0]))
							seqName, ok := dest[1].(*string)
							require.True(t, ok, fmt.Sprintf("sequence name, expected *string, got %T", dest[1]))
							*colName = idColumn
							*seqName = idSequenceName
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantColumns: []string{idColumn},
			wantColumnTableSequences: map[string]map[string]string{
				quotedQualifiedTableName: {idColumn: quoteQualifiedSequenceName(idSequenceName)},
			},
			wantErr: nil,
		},
		{
			name:                 "ok - multiple sequence columns",
			columnTableSequences: map[string]map[string]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					callCount := 0
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn: func(i uint) bool {
							callCount++
							return callCount <= 2
						},
						ScanFn: func(_ uint, dest ...any) error {
							require.Len(t, dest, 2)
							colName, ok := dest[0].(*string)
							require.True(t, ok)
							seqName, ok := dest[1].(*string)
							require.True(t, ok)
							if callCount == 1 {
								*colName = idColumn
								*seqName = "id_seq"
							} else {
								*colName = createdAtColumn
								*seqName = "created_at_seq"
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},

			wantColumns: []string{idColumn, createdAtColumn},
			wantColumnTableSequences: map[string]map[string]string{
				quotedQualifiedTableName: {
					idColumn:        quoteQualifiedSequenceName(idSequenceName),
					createdAtColumn: quoteQualifiedSequenceName(createdAtSequenceName),
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - existing table in cache",
			columnTableSequences: map[string]map[string]string{
				quotedQualifiedTableName: {idColumn: quoteQualifiedSequenceName(idSequenceName)},
			},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errors.New("unexpected call to QueryFn")
				},
			},

			wantColumns: []string{idColumn},
			wantColumnTableSequences: map[string]map[string]string{
				quotedQualifiedTableName: {idColumn: quoteQualifiedSequenceName(idSequenceName)},
			},
			wantErr: nil,
		},
		{
			name:                 "ok - no sequence columns",
			columnTableSequences: map[string]map[string]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return false },
						ScanFn:  func(_ uint, dest ...any) error { return nil },
						ErrFn:   func() error { return nil },
					}, nil
				},
			},

			wantColumns: []string{},
			wantColumnTableSequences: map[string]map[string]string{
				quotedQualifiedTableName: {},
			},
			wantErr: nil,
		},
		{
			name:                 "error - querying table sequences",
			columnTableSequences: map[string]map[string]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantColumns:              nil,
			wantColumnTableSequences: map[string]map[string]string{},
			wantErr:                  errTest,
		},
		{
			name:                 "error - scanning sequence column",
			columnTableSequences: map[string]map[string]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
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

			wantColumns:              nil,
			wantColumnTableSequences: map[string]map[string]string{},
			wantErr:                  errTest,
		},
		{
			name:                 "error - rows error",
			columnTableSequences: map[string]map[string]string{},
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
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

			wantColumns:              nil,
			wantColumnTableSequences: map[string]map[string]string{},
			wantErr:                  errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := &pgSchemaObserver{
				pgConn:               tc.pgConn,
				columnTableSequences: synclib.NewMapFromMap(tc.columnTableSequences),
				logger:               loglib.NewNoopLogger(),
			}

			seqColMap, err := o.getSequenceColumns(context.TODO(), testSchema, testTable)
			require.ErrorIs(t, err, tc.wantErr)
			// Convert map to slice for comparison
			gotColumns := make([]string, 0, len(seqColMap))
			for col := range seqColMap {
				gotColumns = append(gotColumns, col)
			}
			require.ElementsMatch(t, tc.wantColumns, gotColumns)
			require.Equal(t, tc.wantColumnTableSequences, o.columnTableSequences.GetMap())
		})
	}
}

func TestPGSchemaObserver_updateColumnSequences(t *testing.T) {
	t.Parallel()

	defaultVal := func(seqName string) *string {
		val := "nextval('" + seqName + "'::regclass)"
		return &val
	}

	tests := []struct {
		name                     string
		logEntry                 *schemalog.LogEntry
		columnTableSequences     map[string]map[string]string
		wantColumnTableSequences map[string]map[string]string
	}{
		{
			name: "no tables in schema",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema:     schemalog.Schema{},
			},
			columnTableSequences:     map[string]map[string]string{},
			wantColumnTableSequences: map[string]map[string]string{},
		},
		{
			name: "table with no sequence columns",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema: schemalog.Schema{
					Tables: []schemalog.Table{
						{
							Name: "test_table",
							Columns: []schemalog.Column{
								{Name: "name", DataType: "text"},
								{Name: "description", DataType: "text"},
							},
						},
					},
				},
			},
			columnTableSequences: map[string]map[string]string{},
			wantColumnTableSequences: map[string]map[string]string{
				`"test_schema"."test_table"`: {},
			},
		},
		{
			name: "table with sequence columns",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema: schemalog.Schema{
					Tables: []schemalog.Table{
						{
							Name: "test_table",
							Columns: []schemalog.Column{
								{Name: "id", DataType: "bigint", Identity: "a"},
								{Name: "name", DataType: "text"},
								{Name: "sequence_col", DataType: "bigint", DefaultValue: defaultVal("seq")},
							},
						},
					},
				},
			},
			columnTableSequences: map[string]map[string]string{},
			wantColumnTableSequences: map[string]map[string]string{
				`"test_schema"."test_table"`: {`"sequence_col"`: "seq"},
			},
		},
		{
			name: "multiple tables with sequence columns",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema: schemalog.Schema{
					Tables: []schemalog.Table{
						{
							Name: "users",
							Columns: []schemalog.Column{
								{Name: "id", DataType: "bigint", DefaultValue: defaultVal("id_seq")},
								{Name: "name", DataType: "text"},
							},
						},
						{
							Name: "orders",
							Columns: []schemalog.Column{
								{Name: "order_id", DataType: "bigint", DefaultValue: defaultVal("order_id_seq")},
								{Name: "user_id", DataType: "bigint"},
								{Name: "total", DataType: "decimal"},
							},
						},
						{
							Name: "products",
							Columns: []schemalog.Column{
								{Name: "name", DataType: "text"},
								{Name: "price", DataType: "decimal"},
							},
						},
					},
				},
			},
			columnTableSequences: map[string]map[string]string{},
			wantColumnTableSequences: map[string]map[string]string{
				`"test_schema"."users"`:    {`"id"`: "id_seq"},
				`"test_schema"."orders"`:   {`"order_id"`: "order_id_seq"},
				`"test_schema"."products"`: {},
			},
		},
		{
			name: "update existing table sequences",
			logEntry: &schemalog.LogEntry{
				SchemaName: "test_schema",
				Schema: schemalog.Schema{
					Tables: []schemalog.Table{
						{
							Name: "test_table",
							Columns: []schemalog.Column{
								{Name: "new_id", DataType: "bigint", DefaultValue: defaultVal("new_id_seq")},
								{Name: "name", DataType: "text"},
							},
						},
					},
				},
			},
			columnTableSequences: map[string]map[string]string{
				`"test_schema"."test_table"`: {`"old_id"`: "old_id_seq", `"old_sequence"`: "old_sequence_seq"},
			},
			wantColumnTableSequences: map[string]map[string]string{
				`"test_schema"."test_table"`: {`"new_id"`: "new_id_seq"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			obs := &pgSchemaObserver{
				columnTableSequences: synclib.NewMapFromMap(tc.columnTableSequences),
				logger:               loglib.NewNoopLogger(),
			}

			obs.updateColumnSequences(tc.logEntry)
			require.Equal(t, tc.wantColumnTableSequences, obs.columnTableSequences.GetMap())
		})
	}
}

func TestPGSchemaObserver_queryTableSequences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pgConn      pglib.Querier
		wantSeqCols map[string]string
		wantErr     error
	}{
		{
			name: "ok - no sequence columns",
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return false },
						ScanFn:  func(_ uint, dest ...any) error { return nil },
						ErrFn:   func() error { return nil },
					}, nil
				},
			},
			wantSeqCols: map[string]string{},
			wantErr:     nil,
		},
		{
			name: "ok - single sequence column",
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(_ uint, dest ...any) error {
							require.Len(t, dest, 2)
							colName, ok := dest[0].(*string)
							require.True(t, ok, fmt.Sprintf("column name, expected *string, got %T", dest[0]))
							seqName, ok := dest[1].(*string)
							require.True(t, ok, fmt.Sprintf("sequence name, expected *string, got %T", dest[1]))
							*colName = "id"
							*seqName = "id_seq"
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},
			wantSeqCols: map[string]string{`"id"`: `"test_schema"."id_seq"`},
			wantErr:     nil,
		},
		{
			name: "ok - multiple sequence columns",
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					callCount := 0
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn: func(i uint) bool {
							callCount++
							return callCount <= 3
						},
						ScanFn: func(_ uint, dest ...any) error {
							require.Len(t, dest, 2)
							colName, ok := dest[0].(*string)
							require.True(t, ok)
							seqName, ok := dest[1].(*string)
							require.True(t, ok)
							switch callCount {
							case 1:
								*colName = "id"
								*seqName = "id_seq"
							case 2:
								*colName = "order_id"
								*seqName = "order_id_seq"
							case 3:
								*colName = "created_at"
								*seqName = "created_at_seq"
							}
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},
			wantSeqCols: map[string]string{
				`"id"`:         `"test_schema"."id_seq"`,
				`"order_id"`:   `"test_schema"."order_id_seq"`,
				`"created_at"`: `"test_schema"."created_at_seq"`,
			},
			wantErr: nil,
		},
		{
			name: "error - query failed",
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return nil, errTest
				},
			},
			wantSeqCols: nil,
			wantErr:     errTest,
		},
		{
			name: "error - scan failed",
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(_ uint, dest ...any) error {
							return errTest
						},
						ErrFn: func() error { return nil },
					}, nil
				},
			},
			wantSeqCols: nil,
			wantErr:     errTest,
		},
		{
			name: "error - rows error",
			pgConn: &pgmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, sequenceColumnQuery, query)
					require.Equal(t, []any{testSchema, testTable}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn: func(i uint) bool {
							return i == 1
						},
						ScanFn: func(_ uint, dest ...any) error {
							colName, ok := dest[0].(*string)
							require.True(t, ok)
							seqName, ok := dest[1].(*string)
							require.True(t, ok)
							*colName = "id"
							*seqName = "id_seq"
							return nil
						},
						ErrFn: func() error { return errTest },
					}, nil
				},
			},
			wantSeqCols: nil,
			wantErr:     errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := &pgSchemaObserver{
				pgConn: tc.pgConn,
				logger: loglib.NewNoopLogger(),
			}

			seqCols, err := o.queryTableSequences(context.TODO(), tc.pgConn, testSchema, testTable)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantSeqCols, seqCols)
		})
	}
}
