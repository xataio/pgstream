// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers"
	"golang.org/x/exp/slices"
)

type PostgresTransformerParser struct {
	conn           pglib.Querier
	builder        transformerBuilder
	pgtypeMap      *pgtype.Map
	requiredTables []string
}

const (
	fieldDescriptionsQuery = "SELECT * FROM %s LIMIT 0"
	schemaTablesQuery      = "SELECT tablename FROM pg_tables WHERE schemaname=$1"
	publicSchema           = "public"
	wildcard               = "*"
)

var errInvalidTableName = errors.New("invalid table name, expected format: schema.table or table")

func NewPostgresTransformerParser(ctx context.Context, pgURL string, builder transformerBuilder, requiredTables []string) (*PostgresTransformerParser, error) {
	pool, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &PostgresTransformerParser{
		conn:           pool,
		builder:        builder,
		pgtypeMap:      pgtype.NewMap(),
		requiredTables: requiredTables,
	}, nil
}

func (v *PostgresTransformerParser) ParseAndValidate(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
	// validate that all required tables are present in the rules
	if err := v.validateAllRequiredTables(ctx, rules); err != nil {
		return nil, err
	}
	transformerMap := map[string]ColumnTransformers{}
	for _, table := range rules.Transformers {
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
			transformer, err := v.builder.New(cfg)
			if err != nil {
				return nil, err
			}

			// validate that the transformer is compatible with the column type
			if !pgTypeCompatibleWithTransformerType(transformer.CompatibleTypes(), datatype) {
				typeForOid, ok := v.pgtypeMap.TypeForOID(datatype)
				if ok {
					return nil, fmt.Errorf("transformer '%s' specified for column '%s' in table %s does not support pg data type: %s", transformer.Type(), colName, tableKey, typeForOid.Name)
				}
				return nil, fmt.Errorf("transformer '%s' specified for column '%s' in table %s does not support pg data type with oid: %d", transformer.Type(), colName, tableKey, datatype)
			}

			// add the transformer to the map
			schemaTableTransformers[colName] = transformer
		}
	}
	return transformerMap, nil
}

func (v *PostgresTransformerParser) validateAllRequiredTables(ctx context.Context, rules Rules) error {
	if rules.ValidationMode != validationModeStrict {
		// if validation mode is not strict, we don't need to validate required tables
		return nil
	}
	requiredTablesQuoteQualified, err := v.getRequiredTablesList(ctx)
	if err != nil {
		return fmt.Errorf("getting required tables list: %w", err)
	}

	ruleTablesMap := make(map[string]struct{}, len(rules.Transformers))
	for _, table := range rules.Transformers {
		ruleTablesMap[pglib.QuoteQualifiedIdentifier(table.Schema, table.Table)] = struct{}{}
	}

	for _, requiredTable := range requiredTablesQuoteQualified {
		if _, found := ruleTablesMap[requiredTable]; !found {
			return fmt.Errorf("required table %s not found in transformation rules", requiredTable)
		}
	}
	return nil
}

func (v *PostgresTransformerParser) getRequiredTablesList(ctx context.Context) ([]string, error) {
	schemaTablesList := []string{}
	for i := 0; i < len(v.requiredTables); i++ {
		table := v.requiredTables[i]
		schemaName, tableName, err := parseTableName(table)
		if err != nil {
			return nil, err
		}
		if schemaName == wildcard {
			if tableName != wildcard {
				return nil, fmt.Errorf("wildcard schema must be used with wildcard table, got: %q", tableName)
			}

			// if schemaName is wildcard, fetch all schemas
			allSchemas, err := v.getAllSchemaNames(ctx)
			if err != nil {
				return nil, fmt.Errorf("fetching all schemas for wildcard: %w", err)
			}
			for _, schema := range allSchemas {
				v.requiredTables = append(v.requiredTables, schema+"."+wildcard)
			}
			continue
		}

		if tableName != wildcard {
			schemaTablesList = append(schemaTablesList, pglib.QuoteQualifiedIdentifier(schemaName, tableName))
			continue
		}

		// if tableName is wildcard, fetch all tables in the schema
		allTablesInSchema, err := v.getAllSchemaTables(ctx, schemaName)
		if err != nil {
			return nil, fmt.Errorf("fetching all tables for schema %s: %w", schemaName, err)
		}
		schemaTablesList = append(schemaTablesList, allTablesInSchema...)
	}
	return schemaTablesList, nil
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

func (v *PostgresTransformerParser) getAllSchemaTables(ctx context.Context, schema string) ([]string, error) {
	rows, err := v.conn.Query(ctx, schemaTablesQuery, schema)
	if err != nil {
		return nil, fmt.Errorf("fetching all tables for schema %s: %w", schema, err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tableNames = append(tableNames, pglib.QuoteQualifiedIdentifier(schema, tableName))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func (v *PostgresTransformerParser) getAllSchemaNames(ctx context.Context) ([]string, error) {
	const query = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pgstream')"
	rows, err := v.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("discovering all schemas for wildcard: %w", err)
	}
	defer rows.Close()

	schemas := []string{}
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, fmt.Errorf("scanning schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return schemas, nil
}

func parseTableName(qualifiedTableName string) (string, string, error) {
	parts := strings.Split(qualifiedTableName, ".")
	switch len(parts) {
	case 1:
		return publicSchema, parts[0], nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", errInvalidTableName
	}
}

func pgTypeCompatibleWithTransformerType(compatibleTypes []transformers.SupportedDataType, pgType uint32) bool {
	if slices.Contains(compatibleTypes, transformers.AllDataTypes) {
		return true
	}
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
	case pgtype.JSONBOID, pgtype.JSONOID:
		return slices.Contains(compatibleTypes, transformers.JSONDataType)
	default:
		return false
	}
}
