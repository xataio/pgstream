// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/transformers/builder"
)

func TestPostgresTransformerParser_ParseAndValidate(t *testing.T) {
	t.Parallel()
	testSchemaTable := "\"public\".\"test\""
	testQuerier := &pgmocks.Querier{
		QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
			switch query {
			case "SELECT * FROM \"public\".\"test\" LIMIT 0":
				return &pgmocks.Rows{
					FieldDescriptionsFn: func() []pgconn.FieldDescription {
						return []pgconn.FieldDescription{
							{
								Name:        "id",
								DataTypeOID: pgtype.Int8OID,
							},
							{
								Name:        "name",
								DataTypeOID: pgtype.TextOID,
							},
						}
					},
					CloseFn: func() {},
					ErrFn:   func() error { return nil },
				}, nil
			case "SELECT tablename FROM pg_tables WHERE schemaname=$1":
				return &pgmocks.Rows{
					CloseFn: func() {},
					NextFn:  func(i uint) bool { return i == 1 },
					ScanFn: func(dest ...any) error {
						require.Len(t, dest, 1)
						tableName, ok := dest[0].(*string)
						require.True(t, ok)
						*tableName = "test"
						return nil
					},
					ErrFn: func() error { return nil },
				}, nil
			default:
				return nil, fmt.Errorf("unexpected query: %s", query)
			}
		},
	}

	testPGValidator := PostgresTransformerParser{
		conn:           testQuerier,
		builder:        builder.NewTransformerBuilder(),
		pgtypeMap:      pgtype.NewMap(),
		requiredTables: []string{"public.test"},
	}

	tests := []struct {
		name             string
		transformerRules []TableRules
		validator        PostgresTransformerParser

		wantErr             error
		wantTransformersFor []string
	}{
		{
			name: "ok - no error, relaxed mode",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"id": {
							Name: "noop",
						},
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: testPGValidator,

			wantTransformersFor: []string{"name"},
			wantErr:             nil,
		},
		{
			name: "ok - no error for missing column, relaxed mode",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: testPGValidator,

			wantTransformersFor: []string{"name"},
			wantErr:             nil,
		},
		{
			name: "error - missing column for strict validation",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "strict",
					ColumnRules: map[string]TransformerRules{
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: testPGValidator,

			wantErr: fmt.Errorf("column id of table %s has no transformer configured", testSchemaTable),
		},
		{
			name: "error - invalid column type",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"id": {
							Name: "string",
						},
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: testPGValidator,
			wantErr:   errors.New("transformer 'string' specified for column 'id' in table \"public\".\"test\" does not support pg data type: int8"),
		},
		{
			name: "error - column not found in table",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"unknown_column": {
							Name: "string",
						},
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: testPGValidator,
			wantErr:   fmt.Errorf("column %s not found in table %s", "unknown_column", testSchemaTable),
		},
		{
			name: "error - required table not present in rules",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test2",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"id": {
							Name: "string",
						},
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: testPGValidator,
			wantErr:   fmt.Errorf("required table %s not found in transformation rules", "\"public\".\"test\""),
		},
		{
			name: "error - required table not present in rules, validator with wildcard",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test2",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"id": {
							Name: "string",
						},
						"name": {
							Name: "string",
						},
					},
				},
			},
			validator: PostgresTransformerParser{
				conn:           testQuerier,
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pgtype.NewMap(),
				requiredTables: []string{"*"},
			},
			wantErr: fmt.Errorf("required table %s not found in transformation rules", "\"public\".\"test\""),
		},
		{
			name:             "error - invalid table name",
			transformerRules: []TableRules{},
			validator: PostgresTransformerParser{
				conn:           testQuerier,
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pgtype.NewMap(),
				requiredTables: []string{"invalid.table.name"},
			},
			wantErr: errInvalidTableName,
		},
		{
			name:             "error - wildcard schema name",
			transformerRules: []TableRules{},
			validator: PostgresTransformerParser{
				conn:           testQuerier,
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pgtype.NewMap(),
				requiredTables: []string{"*.test"},
			},
			wantErr: fmt.Errorf("wildcard schema name is not supported: *.test"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformerMap, err := tc.validator.ParseAndValidate(tc.transformerRules)
			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)

			columnTransformers, ok := transformerMap[testSchemaTable]
			require.True(t, ok)

			require.Equal(t, len(tc.wantTransformersFor), len(columnTransformers))
			for _, col := range tc.wantTransformersFor {
				require.Contains(t, columnTransformers, col)
			}
		})
	}
}
