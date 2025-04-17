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
	connBuilder connBuilder
}

type connBuilder func(context.Context) (pglib.Querier, error)

func NewPostgresTransformerValidator(pgURL string) *PostgresTransformerValidator {
	return &PostgresTransformerValidator{
		connBuilder: func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConn(ctx, pgURL)
		},
	}
}

func (v *PostgresTransformerValidator) Validate(ctx context.Context, schemaTable string, transformers ColumnTransformers, columns []string) error {
	conn, err := v.connBuilder(ctx)
	if err != nil {
		return fmt.Errorf("creating postgres connection: %w", err)
	}
	defer conn.Close(context.Background())

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", schemaTable)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("querying table rows: %w", err)
	}
	defer rows.Close()
	fieldDescriptions := rows.FieldDescriptions()

	// map column names to column pg type OIDs
	mappedColumns := make(map[string]uint32, len(fieldDescriptions))
	for _, desc := range fieldDescriptions {
		mappedColumns[string(desc.Name)] = desc.DataTypeOID
		if !slices.Contains(columns, string(desc.Name)) {
			// if strict validation is enabled, return error
		}
	}

	// check that all column transformers are compatible with corresponding column types
	for colName, tr := range transformers {
		datatype, found := mappedColumns[colName]
		if !found {
			// validate that all columns in the transformers map are present in the table
			return fmt.Errorf("column %s not found in table %s", colName, schemaTable)
		}
		if !pgTypeCompatibleWithTransformerType(tr.CompatibleTypes(), datatype) {
			return fmt.Errorf("transformer specified for column %s does not support pg data type", colName)
		}
	}

	return nil
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
