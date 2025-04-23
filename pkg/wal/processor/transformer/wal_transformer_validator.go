// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers"
	"golang.org/x/exp/slices"
)

type PostgresTransformerValidator struct {
	conn pglib.Querier
}

func NewPostgresTransformerValidator(ctx context.Context, pgURL string) (*PostgresTransformerValidator, error) {
	pool, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &PostgresTransformerValidator{
		conn: pool,
	}, nil
}

func (v *PostgresTransformerValidator) Validate(ctx context.Context, transformerMap map[string]ColumnTransformers) error {
	for schemaTable, columnTransformers := range transformerMap {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", schemaTable)
		rows, err := v.conn.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("querying table rows: %w", err)
		}
		defer rows.Close()
		fieldDescriptions := rows.FieldDescriptions()

		// TODO: maybe error out if len(fieldDescriptions) != len(columnTransformers)
		// if we start requiring a transformer for every column (noop transformers)

		// map column names to column pg type OIDs, skip columns that don't have a transformer
		mappedColumns := make(map[string]uint32, len(fieldDescriptions))
		for _, desc := range fieldDescriptions {
			if _, found := columnTransformers[string(desc.Name)]; !found {
				continue
			}

			mappedColumns[string(desc.Name)] = desc.DataTypeOID
		}

		// check that all column transformers are compatible with corresponding column types
		for colName, tr := range columnTransformers {
			datatype, found := mappedColumns[colName]
			if !found {
				// validate that all column in the rules are present in the table
				return fmt.Errorf("column %s not found in table %s", colName, schemaTable)
			}
			if !pgTypeCompatibleWithTransformerType(tr.CompatibleTypes(), datatype) {
				return fmt.Errorf("transformer specified for column '%s' in table %s does not support pg data type with oid: %d", colName, schemaTable, datatype)
			}
		}
	}

	return nil
}

func (v *PostgresTransformerValidator) Close() error {
	return v.conn.Close(context.Background())
}

func pgTypeCompatibleWithTransformerType(compatibleTypes []transformers.SupportedDataType, pgType uint32) bool {
	switch pgType {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return slices.Contains(compatibleTypes, transformers.StringDataType)
	case pgtype.Float4OID:
		return slices.Contains(compatibleTypes, transformers.Float32DataType)
	case pgtype.Float8OID:
		return slices.Contains(compatibleTypes, transformers.Float64DataType)
	case pgtype.Int2OID:
		return slices.Contains(compatibleTypes, transformers.Integer16DataType)
	case pgtype.Int4OID:
		return slices.Contains(compatibleTypes, transformers.Integer32DataType)
	case pgtype.Int8OID:
		return slices.Contains(compatibleTypes, transformers.Integer64DataType)
	case pgtype.BoolOID:
		return slices.Contains(compatibleTypes, transformers.BooleanDataType)
	case pgtype.UUIDOID:
		return slices.Contains(compatibleTypes, transformers.UInt8ArrayOf16DataType)
	case pgtype.ByteaOID:
		return slices.Contains(compatibleTypes, transformers.ByteArrayDataType)
	case pgtype.DateOID:
		return slices.Contains(compatibleTypes, transformers.DateDataType)
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return slices.Contains(compatibleTypes, transformers.DatetimeDataType)
	default:
		return false
	}
}
