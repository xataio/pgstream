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
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/mocks"
)

func TestPostgresTransformerValidator(t *testing.T) {
	t.Parallel()
	testSchemaTable := "\"public\".\"test\""
	testQuerier := &pgmocks.Querier{
		QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
			require.Equal(t, "SELECT * FROM \"public\".\"test\" LIMIT 0", query)
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
							Name:        "age",
							DataTypeOID: pgtype.Int2OID,
						},
						{
							Name:        "birth_date",
							DataTypeOID: pgtype.DateOID,
						},
						{
							Name:        "postcode",
							DataTypeOID: pgtype.Int4OID,
						},
						{
							Name:        "customer_id",
							DataTypeOID: pgtype.UUIDOID,
						},
						{
							Name:        "total_purchases",
							DataTypeOID: pgtype.Float8OID,
						},
						{
							Name:        "total_discounts",
							DataTypeOID: pgtype.Float4OID,
						},
						{
							Name:        "is_active",
							DataTypeOID: pgtype.BoolOID,
						},
						{
							Name:        "created_at",
							DataTypeOID: pgtype.TimestamptzOID,
						},
					}
				},
				CloseFn: func() {},
			}, nil
		},
	}

	testPGValidator := PostgresTransformerValidator{
		connBuilder: func(context.Context) (pglib.Querier, error) {
			return testQuerier, nil
		},
	}

	tests := []struct {
		name           string
		wantErr        error
		transformerMap map[string]ColumnTransformers
	}{
		{
			name: "ok - no error, few columns",
			transformerMap: map[string]ColumnTransformers{
				testSchemaTable: {
					"id": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.Integer64DataType,
							}
						},
					},
					"name": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.StringDataType,
							}
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - no error, all columns",
			transformerMap: map[string]ColumnTransformers{
				testSchemaTable: {
					"id": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.Integer64DataType,
							}
						},
					},
					"name": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.StringDataType,
							}
						},
					},
					"age": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.Integer16DataType,
							}
						},
					},
					"birth_date": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.DateDataType,
							}
						},
					},
					"postcode": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.Integer32DataType,
							}
						},
					},
					"customer_id": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.UInt8ArrayOf16DataType,
							}
						},
					},
					"total_purchases": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.Float64DataType,
							}
						},
					},
					"total_discounts": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.Float32DataType,
							}
						},
					},
					"is_active": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.BooleanDataType,
							}
						},
					},
					"created_at": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.DatetimeDataType,
							}
						},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "error - invalid column type",
			transformerMap: map[string]ColumnTransformers{
				testSchemaTable: {
					"id": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.BooleanDataType,
							}
						},
					},
				},
			},
			wantErr: errors.New("transformer specified for column 'id' in table \"public\".\"test\" does not support pg data type with oid: "),
		},
		{
			name: "error - column not found in table",
			transformerMap: map[string]ColumnTransformers{
				testSchemaTable: {
					"unknown_column": &mocks.Transformer{
						CompatibleTypesFn: func() []transformers.SupportedDataType {
							return []transformers.SupportedDataType{
								transformers.StringDataType,
							}
						},
					},
				},
			},
			wantErr: fmt.Errorf("column %s not found in table %s", "unknown_column", testSchemaTable),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := testPGValidator.Validate(context.Background(), tc.transformerMap)
			if !errors.Is(err, tc.wantErr) {
				require.Contains(t, err.Error(), tc.wantErr.Error())
			}
		})
	}
}
