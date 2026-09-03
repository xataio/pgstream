// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	"golang.org/x/exp/slices"
)

type PostgresTransformerParser struct {
	conn           pglib.Querier
	connURL        string
	builder        transformerBuilder
	pgtypeMap      *pglib.Mapper
	requiredTables []string

	warnings []string
	// only a postgres target actually enforces a unique index; elsewhere the
	// same findings are reported but must not block the pipeline
	enforceUniqueness bool
	logger            loglib.Logger
}

type ParserOption func(*PostgresTransformerParser)

// WithUniquenessEnforcement makes transformation rules that break a unique
// index a hard error instead of a warning. Enable it when the target enforces
// unique indexes, which today means a postgres target.
func WithUniquenessEnforcement() ParserOption {
	return func(v *PostgresTransformerParser) {
		v.enforceUniqueness = true
	}
}

// WithParserLogger sets the logger rules validation reports through. It is
// named apart from WithLogger, which configures the transformer processor.
func WithParserLogger(l loglib.Logger) ParserOption {
	return func(v *PostgresTransformerParser) {
		v.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_transformer_parser",
		})
	}
}

const (
	fieldDescriptionsQuery = "SELECT * FROM %s LIMIT 0"
	schemaTablesQuery      = "SELECT tablename FROM pg_tables WHERE schemaname=$1"
	// expression columns have attnum 0 and no pg_attribute row, so the LEFT
	// JOIN yields a NULL attname rather than dropping the index entirely.
	// indkey also carries INCLUDE columns, which do not enforce uniqueness;
	// only the first indnkeyatts entries do
	uniqueIndexQuery = `SELECT idx.relname, i.indisprimary, a.attname
	FROM pg_index i
	JOIN pg_class c ON c.oid = i.indrelid
	JOIN pg_class idx ON idx.oid = i.indexrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
	LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indisunique AND i.indisvalid AND i.indislive
	AND k.ord <= i.indnkeyatts
	AND n.nspname = $1 AND c.relname = $2
	ORDER BY idx.relname, k.ord`
	publicSchema        = "public"
	wildcard            = "*"
	numericTypmodOffset = 4
	choicesParam        = "choices"
)

var (
	errInvalidTableName = errors.New("invalid table name, expected format: schema.table or table")
	// ErrNumericRange is returned when a transformer configured on a numeric
	// column can generate values the column cannot store.
	ErrNumericRange = errors.New("transformer range does not fit the numeric column")
	// ErrInvalidEnumChoice is returned when a greenmask_choice rule on an enum
	// column lists a value the enum does not have.
	ErrInvalidEnumChoice = errors.New("choice is not a valid enum label")
)

// columnType is what rules validation needs to know about a column's type: the
// OID says which transformers accept it, and the modifier carries the
// precision and scale a numeric column constrains its values to.
type columnType struct {
	oid      uint32
	modifier int32
}

func NewPostgresTransformerParser(ctx context.Context, pgURL string, builder transformerBuilder, requiredTables []string, opts ...ParserOption) (*PostgresTransformerParser, error) {
	pool, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	parser := &PostgresTransformerParser{
		conn:           pool,
		connURL:        pgURL,
		builder:        builder,
		pgtypeMap:      pglib.NewMapper(pool),
		requiredTables: requiredTables,
		logger:         loglib.NewNoopLogger(),
	}
	for _, opt := range opts {
		opt(parser)
	}
	return parser, nil
}

func (v *PostgresTransformerParser) Warnings() []string {
	return v.warnings
}

func (v *PostgresTransformerParser) ParseAndValidate(ctx context.Context, rules Rules) (*TransformerMap, error) {
	// reset before any early return, so a failed call cannot leave the
	// previous call's warnings visible through Warnings
	v.warnings = nil

	// validate that all required tables are present in the rules
	if err := v.validateAllRequiredTables(ctx, rules); err != nil {
		return nil, err
	}
	var uniquenessErrs []string
	transformerMap := NewTransformerMap()
	for _, table := range rules.Transformers {
		fieldDescriptions, err := v.getFieldDescriptions(context.Background(), table.Schema, table.Table)
		if err != nil {
			return nil, err
		}

		// map column names to their pg type OID and modifier
		mappedColumnTypes := make(map[string]columnType, len(fieldDescriptions))
		for _, desc := range fieldDescriptions {
			if _, found := table.ColumnRules[string(desc.Name)]; !found {
				// column is not configured in rules, error out if strict validation mode is enabled
				if table.ValidationMode == validationModeStrict {
					return nil, fmt.Errorf("column %s of table %q.%q has no transformer configured", desc.Name, table.Schema, table.Table)
				}
				continue
			}
			mappedColumnTypes[string(desc.Name)] = columnType{oid: desc.DataTypeOID, modifier: desc.TypeModifier}
		}

		for colName, transformerRules := range table.ColumnRules {
			cfg := transformerRulesToConfig(transformerRules)

			if cfg.Name == "" || cfg.Name == "noop" {
				transformerMap.AddNoopTransformer(table.Schema, table.Table, colName)
				continue
			}

			// get the data type so that we can later validate if it's compatible with the configured transformer
			colType, found := mappedColumnTypes[colName]
			if !found {
				// validate that the column in the rules is present in the table
				return nil, fmt.Errorf("column %s not found in table %q.%q", colName, table.Schema, table.Table)
			}

			// resolved before the transformer is built, since an enum column
			// supplies the choices a transformer can be built with
			enum, err := v.pgtypeMap.EnumForOID(ctx, colType.oid)
			if err != nil {
				return nil, fmt.Errorf("column '%s' in table %q.%q: resolving enum type: %w", colName, table.Schema, table.Table, err)
			}

			switch cfg.Name {
			case transformers.PGAnonymizer:
				// pg_anonymizer transformer requires a connection pool, set
				// the source PG URL if not provided
				if cfg.Parameters["postgres_url"] == nil {
					cfg.Parameters["postgres_url"] = v.connURL
				}
			case transformers.GreenmaskChoice:
				defaulted, err := v.applyEnumChoices(cfg, enum)
				if err != nil {
					return nil, fmt.Errorf("column '%s' in table %q.%q: %w", colName, table.Schema, table.Table, err)
				}
				if defaulted {
					v.reportDefaultedChoices(table.Schema, table.Table, colName, cfg, enum)
				}
			}

			// build the transformer
			transformer, err := v.builder.New(cfg)
			if err != nil {
				return nil, fmt.Errorf("column '%s' in table %q.%q: %w", colName, table.Schema, table.Table, err)
			}

			dataTypeName, err := v.pgtypeMap.TypeForOID(ctx, colType.oid)

			// validate that the transformer is compatible with the column type
			if err != nil || !pgTypeCompatibleWithTransformerType(transformer.CompatibleTypes(), colType.oid, dataTypeName, enum) {
				return nil, fmt.Errorf("transformer '%s' specified for column '%s' in table %q.%q does not support pg data type: %s with OID: %d", transformer.Type(), colName, table.Schema, table.Table, dataTypeName, colType.oid)
			}

			if err := validateNumericRange(cfg, colType); err != nil {
				return nil, fmt.Errorf("column '%s' in table %q.%q: %w", colName, table.Schema, table.Table, err)
			}

			// add the transformer to the map
			transformerMap.AddActiveTransformer(table.Schema, table.Table, colName, transformer)
		}

		// catch collisions before the load
		uniqueIndexes, err := v.getUniqueIndexes(ctx, table.Schema, table.Table)
		if err != nil {
			return nil, err
		}
		columnTransformers, _ := transformerMap.GetActiveColumnTransformers(table.Schema, table.Table)
		findings := validateUniqueness(table.Schema, table.Table, uniqueIndexes, columnTransformers, allowUniquenessLossColumns(table))
		if v.enforceUniqueness {
			uniquenessErrs = append(uniquenessErrs, findings.errors...)
		} else {
			v.warnings = append(v.warnings, findings.errors...)
		}
		v.warnings = append(v.warnings, findings.warnings...)
	}

	if len(uniquenessErrs) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrUniquenessNotPreserved, strings.Join(uniquenessErrs, "; "))
	}

	return transformerMap, nil
}

func allowUniquenessLossColumns(table TableRules) map[string]bool {
	allowed := make(map[string]bool, len(table.ColumnRules))
	for colName, colRules := range table.ColumnRules {
		if colRules.AllowUniquenessLoss {
			allowed[colName] = true
		}
	}
	return allowed
}

func (v *PostgresTransformerParser) getUniqueIndexes(ctx context.Context, schema, table string) ([]uniqueIndex, error) {
	rows, err := v.conn.Query(ctx, uniqueIndexQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("querying unique indexes for table %q.%q: %w", schema, table, err)
	}
	defer rows.Close()

	// rows arrive grouped by index
	var indexes []uniqueIndex
	for rows.Next() {
		var indexName string
		var columnName *string
		var primary bool
		if err := rows.Scan(&indexName, &primary, &columnName); err != nil {
			return nil, fmt.Errorf("scanning unique index for table %q.%q: %w", schema, table, err)
		}
		if len(indexes) == 0 || indexes[len(indexes)-1].name != indexName {
			indexes = append(indexes, uniqueIndex{name: indexName, primary: primary})
		}
		current := &indexes[len(indexes)-1]
		if columnName == nil {
			current.hasExpressions = true
			continue
		}
		current.columns = append(current.columns, *columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading unique indexes for table %q.%q: %w", schema, table, err)
	}

	return indexes, nil
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

func (v *PostgresTransformerParser) getFieldDescriptions(ctx context.Context, schema, table string) ([]pgconn.FieldDescription, error) {
	query := fmt.Sprintf(fieldDescriptionsQuery, pglib.QuoteQualifiedIdentifier(schema, table))
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
	const query = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pgstream') AND nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast_temp_%'"
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

func pgTypeCompatibleWithTransformerType(compatibleTypes []transformers.SupportedDataType, pgTypeOID uint32, pgTypeName string, enum *pglib.EnumType) bool {
	if slices.Contains(compatibleTypes, transformers.AllDataTypes) {
		return true
	}
	// an enum accepts only a transformer that can be constrained to its
	// labels, and never through the OID switch below, since its OID is
	// assigned by the database rather than fixed. An array of an enum has its
	// own OID and resolves to no enum, so it falls through and is rejected by
	// name like any other array
	if enum != nil {
		return slices.Contains(compatibleTypes, transformers.EnumDataType)
	}
	switch pgTypeOID {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return slices.Contains(compatibleTypes, transformers.StringDataType)
	case pgtype.Float4OID:
		return slices.Contains(compatibleTypes, transformers.Float32DataType)
	case pgtype.Float8OID, pgtype.NumericOID:
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
		// handle extension/custom supported types
		switch pgTypeName {
		case "citext":
			return slices.Contains(compatibleTypes, transformers.CitextDataType)
		case "hstore":
			return slices.Contains(compatibleTypes, transformers.HstoreDataType)
		default:
			return false
		}
	}
}

func (c columnType) numericPrecisionScale() (precision, scale int, ok bool) {
	if c.oid != pgtype.NumericOID || c.modifier < numericTypmodOffset {
		return 0, 0, false
	}
	typmod := c.modifier - numericTypmodOffset
	return int(typmod>>16) & 0xffff, int(typmod) & 0xffff, true
}

// validateNumericRange checks that a transformer configured on a numeric
// column cannot generate a value the column will reject.
func validateNumericRange(cfg *transformers.Config, colType columnType) error {
	if cfg.Name != transformers.GreenmaskFloat && cfg.Name != transformers.GreenmaskInteger {
		return nil
	}
	precision, scale, ok := colType.numericPrecisionScale()
	if !ok {
		return nil
	}

	// the largest magnitude a numeric(p,s) can hold, exclusive
	limit := math.Pow(10, float64(precision-scale))

	for _, param := range []string{"min_value", "max_value"} {
		value, found := cfg.Parameters[param]
		if !found {
			return fmt.Errorf("%w: %q defaults to the full range of its type, which does not fit numeric(%d,%d); set it explicitly",
				ErrNumericRange, param, precision, scale)
		}
		magnitude, err := numericParamMagnitude(value)
		if err != nil {
			return fmt.Errorf("%w: %q: %w", ErrNumericRange, param, err)
		}
		if magnitude >= limit {
			return fmt.Errorf("%w: %q is %g, which does not fit numeric(%d,%d) (maximum magnitude %g)",
				ErrNumericRange, param, magnitude, precision, scale, limit)
		}
	}
	return nil
}

func numericParamMagnitude(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return math.Abs(v), nil
	case float32:
		return math.Abs(float64(v)), nil
	case int:
		return math.Abs(float64(v)), nil
	case int64:
		return math.Abs(float64(v)), nil
	default:
		return 0, fmt.Errorf("got %T, want a number", value)
	}
}

// applyEnumChoices reconciles a greenmask_choice rule with the column it is
// configured on. On an enum column the labels are the natural choices, so a
// rule that omits them gets them, and a rule that lists them is checked
// against the enum now rather than failing per row against the target. It
// reports whether the choices were defaulted.
//
// The labels are read once, here, so a label renamed on the source afterwards
// is only picked up on restart. Constraints narrowing the column further, such
// as a CHECK on a domain over the enum, are not visible in the row description
// this resolves from and are not honoured; list the choices explicitly there.
func (v *PostgresTransformerParser) applyEnumChoices(cfg *transformers.Config, enum *pglib.EnumType) (bool, error) {
	if enum == nil {
		return false, nil
	}

	choices, found, err := transformers.FindParameterArray[string](cfg.Parameters, choicesParam)
	if err != nil {
		return false, fmt.Errorf("%s must be an array of strings: %w", choicesParam, err)
	}
	if !found {
		if cfg.Parameters == nil {
			cfg.Parameters = transformers.ParameterValues{}
		}
		// cloned: the labels belong to the mapper's cache, which every column
		// of this enum shares
		cfg.Parameters[choicesParam] = slices.Clone(enum.Labels)
		return true, nil
	}

	// an explicitly empty list is a mistake, most often a template that
	// rendered nothing, so let the builder reject it rather than silently
	// widening it to every label
	for _, choice := range choices {
		if !slices.Contains(enum.Labels, choice) {
			return false, fmt.Errorf("%w: %q is not a label of enum %q (%s)",
				ErrInvalidEnumChoice, choice, enum.Name, strings.Join(enum.Labels, ", "))
		}
	}
	return false, nil
}

// reportDefaultedChoices records a choice set the operator never wrote. It is
// the only trace of it: the labels live in memory, and a value the target
// later rejects cannot otherwise be traced back to a rule.
func (v *PostgresTransformerParser) reportDefaultedChoices(schema, table, column string, cfg *transformers.Config, enum *pglib.EnumType) {
	v.logger.Info("defaulting greenmask_choice choices to the enum's labels", loglib.Fields{
		"schema": schema, "table": table, "column": column,
		"enum": enum.Name, "choices": enum.Labels,
	})

	// the deterministic generator maps each label to a fixed other label with
	// no secret involved, and an enum publishes its whole label set to anyone
	// who can read the target, so the mapping is trivially invertible
	if generator, _ := cfg.Parameters["generator"].(string); generator == "deterministic" {
		v.warnings = append(v.warnings, fmt.Sprintf(
			"%s: column %q uses greenmask_choice with the deterministic generator over enum %q's own labels, which is reversible by anyone who can read the target; use generator: random to break the correspondence",
			schemaTableKey(schema, table), column, enum.Name))
	}
}
