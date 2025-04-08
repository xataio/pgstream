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

func PgTransformerValidator(ctx context.Context, pgurl string) ValidatorFn {
	return func(transformerMap map[string]ColumnTransformers) error {
		conn, err := pglib.NewConn(ctx, pgurl)
		if err != nil {
			return fmt.Errorf("creating postgres connection pool: %w", err)
		}
		defer conn.Close(context.Background())

		for schemaTable, columnTransformers := range transformerMap {
			query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", schemaTable)
			rows, err := conn.Query(ctx, query)
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
					return fmt.Errorf("column %s not found in table %s", colName, schemaTable)
				}
				if !pgTypeCompatibleWithTransformerType(tr.CompatibleTypes(), datatype) {
					return fmt.Errorf("transformer specified for column %s does not support pg data type", colName)
				}
			}
		}

		return nil
	}
}

func pgTypeCompatibleWithTransformerType(compatibleTypes []transformers.SupportedDataType, pgType uint32) bool {
	switch pgType {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return slices.Contains(compatibleTypes, transformers.StringDataType)
	case pgtype.Float4OID:
		return slices.Contains(compatibleTypes, transformers.Float32)
	case pgtype.Float8OID:
		return slices.Contains(compatibleTypes, transformers.Float64)
	case pgtype.Int2OID:
		return slices.Contains(compatibleTypes, transformers.Integer16)
	case pgtype.Int4OID:
		return slices.Contains(compatibleTypes, transformers.Integer32)
	case pgtype.Int8OID:
		return slices.Contains(compatibleTypes, transformers.Integer64)
	case pgtype.BoolOID:
		return slices.Contains(compatibleTypes, transformers.Boolean)
	case pgtype.UUIDOID:
		return slices.Contains(compatibleTypes, transformers.UInt8ArrayOf16)
	case pgtype.ByteaOID:
		return slices.Contains(compatibleTypes, transformers.ByteArray)
	case pgtype.DateOID:
		return slices.Contains(compatibleTypes, transformers.Date)
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return slices.Contains(compatibleTypes, transformers.Datetime)
	default:
		return false
	}
}
