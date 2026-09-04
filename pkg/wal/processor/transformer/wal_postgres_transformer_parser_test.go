// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
)

func TestPostgresTransformerParser_ParseAndValidate(t *testing.T) {
	t.Parallel()

	citextOID := uint32(1234)
	citextTypeName := "citext"
	testSchemaTable := "\"public\".\"test\""
	testQuerier := func() *pgmocks.Querier {
		return &pgmocks.Querier{
			QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
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
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 1)
							tableName, ok := dest[0].(*string)
							require.True(t, ok)
							*tableName = "test"
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				case uniqueIndexQuery:
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return false },
						ErrFn:   func() error { return nil },
					}, nil
				case "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pgstream') AND nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast_temp_%'":
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
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
			QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
				switch query {
				case "SELECT typname FROM pg_type WHERE oid = $1":
					require.Equal(t, 1, len(args))
					require.Equal(t, citextOID, args[0])
					require.Len(t, dest, 1)
					dataTypeName, ok := dest[0].(*string)
					require.True(t, ok)
					*dataTypeName = citextTypeName
					return nil
				default:
					return nil
				}
			},
		}
	}

	testQuerierWithUnknownTypeErr := testQuerier()
	testQuerierWithUnknownTypeErr.QueryRowFn = func(ctx context.Context, dest []any, query string, args ...any) error {
		require.Equal(t, query, "SELECT typname FROM pg_type WHERE oid = $1")
		require.Equal(t, 1, len(args))
		require.Equal(t, citextOID, args[0])
		return errors.New("not found")
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

		wantErr                   error
		wantActiveTransformersFor []string
		wantNoopTransformersFor   []string
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

			wantActiveTransformersFor: []string{"name"},
			wantNoopTransformersFor:   []string{"id"},
			wantErr:                   nil,
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

			wantActiveTransformersFor: []string{"name"},
			wantErr:                   nil,
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

			wantActiveTransformersFor: []string{"id", "name"},
			wantNoopTransformersFor:   []string{"email"},
			wantErr:                   nil,
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

			wantActiveTransformersFor: []string{"id", "name"},
			wantNoopTransformersFor:   []string{"email"},
			wantErr:                   nil,
		},
		{
			name: "ok - email transformer",
			transformerRules: []TableRules{
				{
					Schema:         "public",
					Table:          "test",
					ValidationMode: "relaxed",
					ColumnRules: map[string]TransformerRules{
						"email": {
							Name: "email",
						},
					},
				},
			},
			validator:                 testPGValidator,
			wantActiveTransformersFor: []string{"email"},
			wantErr:                   nil,
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
			validator:                 testPGValidator,
			wantActiveTransformersFor: []string{"email"},
			wantErr:                   nil,
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

			activeColumnTransformers, ok := transformerMap.GetActiveColumnTransformers(`"public"`, `"test"`)
			require.True(t, ok)
			require.Equal(t, len(tc.wantActiveTransformersFor), len(activeColumnTransformers))
			for _, col := range tc.wantActiveTransformersFor {
				require.Contains(t, activeColumnTransformers, col)
			}

			noopColumnTransformers, _ := transformerMap.GetNoopColumnTransformers(`"public"`, `"test"`)
			require.Equal(t, len(tc.wantNoopTransformersFor), len(noopColumnTransformers))
			for _, col := range tc.wantNoopTransformersFor {
				require.Contains(t, noopColumnTransformers, col)
			}
		})
	}
}

func TestPostgresTransformerParser_uniqueIndexValidation(t *testing.T) {
	t.Parallel()

	// two indexes over the same table, so the row-grouping loop has to start a
	// new index when the name changes and keep column order within each
	type indexRow struct {
		index   string
		primary bool
		column  *string
	}
	name, email := "name", "email"
	indexRows := []indexRow{
		{index: "test_name_email_key", column: &name},
		{index: "test_name_email_key", column: &email},
		{index: "test_pkey", primary: true, column: &email},
	}

	testQuerier := func() *pgmocks.Querier {
		return &pgmocks.Querier{
			QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
				switch query {
				case "SELECT * FROM \"public\".\"test\" LIMIT 0":
					return &pgmocks.Rows{
						FieldDescriptionsFn: func() []pgconn.FieldDescription {
							return []pgconn.FieldDescription{
								{Name: "name", DataTypeOID: pgtype.TextOID},
								{Name: "email", DataTypeOID: pgtype.TextOID},
							}
						},
						CloseFn: func() {},
						ErrFn:   func() error { return nil },
					}, nil
				case uniqueIndexQuery:
					require.Equal(t, []any{"public", "test"}, args)
					return &pgmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i <= uint(len(indexRows)) },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 3)
							indexName, ok := dest[0].(*string)
							require.True(t, ok)
							primary, ok := dest[1].(*bool)
							require.True(t, ok)
							columnName, ok := dest[2].(**string)
							require.True(t, ok)
							row := indexRows[i-1]
							*indexName = row.index
							*primary = row.primary
							*columnName = row.column
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				default:
					return nil, fmt.Errorf("unexpected query: %s", query)
				}
			},
		}
	}

	tests := []struct {
		name        string
		columnRules map[string]TransformerRules
		enforce     bool

		wantErr      error
		wantErrMsg   string
		wantWarnings []string
	}{
		{
			name: "error - masking on a column of the unique index",
			columnRules: map[string]TransformerRules{
				"name": {Name: "masking", Parameters: map[string]any{"type": "id"}},
			},
			enforce:    true,
			wantErr:    ErrUniquenessNotPreserved,
			wantErrMsg: `unique index "test_name_email_key" (name, email) is covered by a transformer that maps distinct values to the same output ("name" uses "masking")`,
		},
		{
			name: "warning - masking is not enforced without a postgres target",
			columnRules: map[string]TransformerRules{
				"name": {Name: "masking", Parameters: map[string]any{"type": "id"}},
			},
			wantWarnings: []string{
				`"public"."test": unique index "test_name_email_key" (name, email) is covered by a transformer that maps distinct values to the same output ("name" uses "masking"), ` +
					`which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, or set allow_uniqueness_loss on the column to override`,
			},
		},
		{
			name: "error - masking on the primary key column",
			columnRules: map[string]TransformerRules{
				"email": {Name: "masking", Parameters: map[string]any{"type": "id"}},
			},
			enforce:    true,
			wantErr:    ErrUniquenessNotPreserved,
			wantErrMsg: `primary key "test_pkey" (email) is covered by a transformer`,
		},
		{
			name: "ok - masking with allow_uniqueness_loss",
			columnRules: map[string]TransformerRules{
				"name": {Name: "masking", Parameters: map[string]any{"type": "id"}, AllowUniquenessLoss: true},
			},
			enforce: true,
		},
		{
			name: "ok - encrypted_aes_siv preserves uniqueness",
			columnRules: map[string]TransformerRules{
				"name": {Name: "encrypted_aes_siv", Parameters: map[string]any{"key_hex": strings.Repeat("ab", 64)}},
			},
			enforce: true,
		},
		{
			name: "warning - random string does not guarantee uniqueness",
			columnRules: map[string]TransformerRules{
				"name": {Name: "string"},
			},
			enforce: true,
			wantWarnings: []string{
				`"public"."test": unique index "test_name_email_key" (name, email) is covered by a transformer that does not guarantee unique output ("name" uses "string"), so duplicate key violations are possible`,
			},
		},
		{
			name: "ok - noop rule on a column of the unique index",
			columnRules: map[string]TransformerRules{
				"name": {Name: "noop"},
			},
			enforce: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parser := PostgresTransformerParser{
				conn:              testQuerier(),
				builder:           builder.NewTransformerBuilder(),
				pgtypeMap:         pglib.NewMapper(testQuerier()),
				enforceUniqueness: tc.enforce,
			}

			_, err := parser.ParseAndValidate(context.Background(), Rules{
				ValidationMode: validationModeRelaxed,
				Transformers: []TableRules{
					{
						Schema:         "public",
						Table:          "test",
						ValidationMode: validationModeRelaxed,
						ColumnRules:    tc.columnRules,
					},
				},
			})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErrMsg != "" {
				require.Contains(t, err.Error(), tc.wantErrMsg)
			}
			require.Equal(t, tc.wantWarnings, parser.Warnings())
		})
	}
}

func Test_pgTypeCompatibleWithTransformerType(t *testing.T) {
	t.Parallel()

	floatCompatible := []transformers.SupportedDataType{
		transformers.Float32DataType,
		transformers.Float64DataType,
	}
	stringCompatible := []transformers.SupportedDataType{transformers.StringDataType}

	tests := []struct {
		name            string
		compatibleTypes []transformers.SupportedDataType
		oid             uint32
		typeName        string
		want            bool
	}{
		{
			name:            "numeric with a float compatible transformer",
			compatibleTypes: floatCompatible,
			oid:             pgtype.NumericOID,
			typeName:        "numeric",
			want:            true,
		},
		{
			name:            "numeric with a string only transformer",
			compatibleTypes: stringCompatible,
			oid:             pgtype.NumericOID,
			typeName:        "numeric",
			want:            false,
		},
		{
			name:            "numeric with a transformer supporting all types",
			compatibleTypes: []transformers.SupportedDataType{transformers.AllDataTypes},
			oid:             pgtype.NumericOID,
			typeName:        "numeric",
			want:            true,
		},
		{
			name:            "float8 with a float compatible transformer",
			compatibleTypes: floatCompatible,
			oid:             pgtype.Float8OID,
			typeName:        "float8",
			want:            true,
		},
		{
			name:            "numeric array is not covered",
			compatibleTypes: floatCompatible,
			oid:             pgtype.NumericArrayOID,
			typeName:        "_numeric",
			want:            false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, pgTypeCompatibleWithTransformerType(tc.compatibleTypes, tc.oid, tc.typeName))
		})
	}
}

func Test_validateNumericRange(t *testing.T) {
	t.Parallel()

	// numeric(9,6): three digits before the point, so magnitudes below 1000
	constrained := columnType{oid: pgtype.NumericOID, modifier: ((9 << 16) | 6) + 4}
	unconstrained := columnType{oid: pgtype.NumericOID, modifier: -1}
	float8 := columnType{oid: pgtype.Float8OID, modifier: -1}

	tests := []struct {
		name    string
		cfg     *transformers.Config
		colType columnType
		wantErr error
	}{
		{
			name: "range fits the column",
			cfg: &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{
				"min_value": -90.0, "max_value": 90.0,
			}},
			colType: constrained,
		},
		{
			name: "range exceeds the column precision",
			cfg: &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{
				"min_value": 0.0, "max_value": 100000.0,
			}},
			colType: constrained,
			wantErr: ErrNumericRange,
		},
		{
			name: "negative bound exceeds the column precision",
			cfg: &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{
				"min_value": -100000.0, "max_value": 0.0,
			}},
			colType: constrained,
			wantErr: ErrNumericRange,
		},
		{
			// the transformer default spans the whole float32 range, which
			// fits no realistic numeric(p,s)
			name:    "bounds omitted on a constrained column",
			cfg:     &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{}},
			colType: constrained,
			wantErr: ErrNumericRange,
		},
		{
			name: "integer bounds are accepted",
			cfg: &transformers.Config{Name: transformers.GreenmaskInteger, Parameters: transformers.ParameterValues{
				"min_value": 0, "max_value": 500,
			}},
			colType: constrained,
		},
		{
			name: "integer bounds exceeding the column precision",
			cfg: &transformers.Config{Name: transformers.GreenmaskInteger, Parameters: transformers.ParameterValues{
				"min_value": 0, "max_value": 5000,
			}},
			colType: constrained,
			wantErr: ErrNumericRange,
		},
		{
			name: "a non numeric bound is rejected",
			cfg: &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{
				"min_value": "zero", "max_value": 90.0,
			}},
			colType: constrained,
			wantErr: ErrNumericRange,
		},
		{
			// nothing to check against: any generated value fits
			name:    "unconstrained numeric is not checked",
			cfg:     &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{}},
			colType: unconstrained,
		},
		{
			name:    "a non numeric column is not checked",
			cfg:     &transformers.Config{Name: transformers.GreenmaskFloat, Parameters: transformers.ParameterValues{}},
			colType: float8,
		},
		{
			// its bounds are timestamps, not numbers
			name:    "greenmask_unix_timestamp is not checked",
			cfg:     &transformers.Config{Name: transformers.GreenmaskUnixTimestamp, Parameters: transformers.ParameterValues{}},
			colType: constrained,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, validateNumericRange(tc.cfg, tc.colType), tc.wantErr)
		})
	}
}
