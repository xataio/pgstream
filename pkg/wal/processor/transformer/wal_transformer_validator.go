// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
	"golang.org/x/exp/slices"
)

type PostgresTransformerParser struct {
	conn pglib.Querier
}

const fieldDescriptionsQuery = "SELECT * FROM %s LIMIT 0"

func NewPostgresTransformerParser(ctx context.Context, pgURL string) (*PostgresTransformerParser, error) {
	pool, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &PostgresTransformerParser{
		conn: pool,
	}, nil
}

func (v *PostgresTransformerParser) ParseAndValidate(rules []TableRules) (map[string]ColumnTransformers, error) {
	transformerMap := map[string]ColumnTransformers{}
	for _, table := range rules {
		tableKey := schemaTableKey(table.Schema, table.Table)
		fieldDescriptions, err := v.getFieldDescriptions(context.Background(), tableKey)
		if err != nil {
			return nil, err
		}

		// map column names to column pg type OIDs
		mappedColumnTypes := make(map[string]uint32, len(fieldDescriptions))
		for _, desc := range fieldDescriptions {
			if _, found := table.ColumnRules[string(desc.Name)]; !found {
				// column is not configured in rules, error out if strict validation mode is enabled
				if table.ValidationMode == validationModeStrict {
					return nil, fmt.Errorf("column %s of table %s has no transformer configured", desc.Name, tableKey)
				}
				continue
			}
			mappedColumnTypes[string(desc.Name)] = desc.DataTypeOID
		}

		schemaTableTransformers := make(map[string]transformers.Transformer)
		transformerMap[tableKey] = schemaTableTransformers
		for colName, transformerRules := range table.ColumnRules {
			// get the data type so that we can later validate if it's compatible with the configured transformer
			datatype, found := mappedColumnTypes[colName]
			if !found {
				// validate that the column in the rules is present in the table
				return nil, fmt.Errorf("column %s not found in table %s", colName, tableKey)
			}

			cfg := transformerRulesToConfig(transformerRules)

			// skip if noop transformer
			if cfg.Name == "" || cfg.Name == "noop" {
				continue
			}

			// build the transformer
			transformer, err := builder.New(cfg)
			if err != nil {
				return nil, err
			}

			// validate that the transformer is compatible with the column type
			if !pgTypeCompatibleWithTransformerType(transformer.CompatibleTypes(), datatype) {
				return nil, fmt.Errorf("transformer specified for column '%s' in table %s does not support pg data type with oid: %d", colName, tableKey, datatype)
			}

			// add the transformer to the map
			schemaTableTransformers[colName] = transformer
		}
	}
	return transformerMap, nil
}

func (v *PostgresTransformerParser) Close() error {
	return v.conn.Close(context.Background())
}

func (v *PostgresTransformerParser) getFieldDescriptions(ctx context.Context, schemaTable string) ([]pgconn.FieldDescription, error) {
	query := fmt.Sprintf(fieldDescriptionsQuery, schemaTable)
	rows, err := v.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying table rows: %w", err)
	}
	defer rows.Close()
	return rows.FieldDescriptions(), rows.Err()
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
	case pgtype.JSONBOID:
		return slices.Contains(compatibleTypes, transformers.JSONDataType)
	default:
		return false
	}
}
