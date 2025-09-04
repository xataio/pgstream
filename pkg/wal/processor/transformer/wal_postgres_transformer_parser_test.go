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

	citextOID := uint32(1234)
	citextTypeName := "citext"
	testSchemaTable := "\"public\".\"test\""
	testQuerier := func() *pgmocks.Querier {
		return &pgmocks.Querier{
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
								{
									Name:        "email",
									DataTypeOID: 1234, // fake OID to be mapped
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
				case "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pgstream')":
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 1)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = "public"
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				default:
					return nil, fmt.Errorf("unexpected query: %s", query)
				}
			},
			QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
				switch query {
				case "SELECT typname FROM pg_type WHERE oid = $1":
					require.Equal(t, 1, len(args))
					require.Equal(t, citextOID, args[0])
					return &pgmocks.Row{
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 1)
							dataTypeName, ok := dest[0].(*string)
							require.True(t, ok)
							*dataTypeName = citextTypeName
							return nil
						},
					}
				default:
					return nil
				}
			},
		}
	}

	testQuerierWithUnknownTypeErr := testQuerier()
	testQuerierWithUnknownTypeErr.QueryRowFn = func(ctx context.Context, query string, args ...any) pglib.Row {
		return &pgmocks.Row{
			ScanFn: func(dest ...any) error {
				require.Equal(t, query, "SELECT typname FROM pg_type WHERE oid = $1")
				require.Equal(t, 1, len(args))
				require.Equal(t, citextOID, args[0])
				return errors.New("not found")
			},
		}
	}

	testPGValidator := PostgresTransformerParser{
		conn:           testQuerier(),
		builder:        builder.NewTransformerBuilder(),
		pgtypeMap:      pglib.NewMapper(testQuerier()),
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
			name: "ok - with wildcard table",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "strict",
					ColumnRules: map[string]TransformerRules{
						"id": {
							Name: "greenmask_integer",
						},
						"name": {
							Name: "string",
						},
						"email": {
							Name: "noop",
						},
					},
				},
			},
			validator: PostgresTransformerParser{
				conn:           testQuerier(),
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pglib.NewMapper(testQuerier()),
				requiredTables: []string{"*"},
			},

			wantTransformersFor: []string{"id", "name"},
			wantErr:             nil,
		},
		{
			name: "ok - with wildcard schema and table",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "strict",
					ColumnRules: map[string]TransformerRules{
						"id": {
							Name: "greenmask_integer",
						},
						"name": {
							Name: "string",
						},
						"email": {
							Name: "noop",
						},
					},
				},
			},
			validator: PostgresTransformerParser{
				conn:           testQuerier(),
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pglib.NewMapper(testQuerier()),
				requiredTables: []string{"*.*"},
			},

			wantTransformersFor: []string{"id", "name"},
			wantErr:             nil,
		},
		{
			name: "ok - custom type",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"email": {
							Name: "neosync_email",
						},
					},
				},
			},
			validator:           testPGValidator,
			wantTransformersFor: []string{"email"},
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
			wantErr:   errors.New("transformer 'string' specified for column 'id' in table \"public\".\"test\" does not support pg data type: int8 with OID: 20"),
		},
		{
			name: "error - unknown custom column type",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"email": {
							Name: "neosync_email",
						},
					},
				},
			},
			validator: PostgresTransformerParser{
				conn:           testQuerierWithUnknownTypeErr,
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pglib.NewMapper(testQuerierWithUnknownTypeErr),
				requiredTables: []string{"public.test"},
			},
			wantErr: errors.New("transformer 'neosync_email' specified for column 'email' in table \"public\".\"test\" does not support pg data type: unknown with OID: 1234"),
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
				conn:           testQuerier(),
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pglib.NewMapper(testQuerier()),
				requiredTables: []string{"*"},
			},
			wantErr: fmt.Errorf("required table %s not found in transformation rules", "\"public\".\"test\""),
		},
		{
			name:             "error - invalid table name",
			transformerRules: []TableRules{},
			validator: PostgresTransformerParser{
				conn:           testQuerier(),
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pglib.NewMapper(testQuerier()),
				requiredTables: []string{"invalid.table.name"},
			},
			wantErr: errInvalidTableName,
		},
		{
			name:             "error - wildcard schema name with non-wildcard table name",
			transformerRules: []TableRules{},
			validator: PostgresTransformerParser{
				conn:           testQuerier(),
				builder:        builder.NewTransformerBuilder(),
				pgtypeMap:      pglib.NewMapper(testQuerier()),
				requiredTables: []string{"*.test"},
			},
			wantErr: fmt.Errorf("getting required tables list: wildcard schema must be used with wildcard table, got: \"test\""),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformerMap, err := tc.validator.ParseAndValidate(context.Background(), Rules{Transformers: tc.transformerRules, ValidationMode: validationModeStrict})
			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Equal(t, err.Error(), tc.wantErr.Error())
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
