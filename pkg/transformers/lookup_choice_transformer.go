// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers/generators"
	"github.com/xataio/pgstream/pkg/transformers/internal/lookup"
)

// LookupChoiceTransformer replaces a value with one taken from a column of
// another table. The values are read once, when the transformer is built, so
// the configuration does not have to be regenerated when the lookup table
// changes.
type LookupChoiceTransformer struct {
	values          []any
	generator       generators.Generator
	compatibleTypes []SupportedDataType
	// dateColumn renders time.Time values the way the replication path
	// delivers a date, so both ingestion paths hash to the same key
	dateColumn bool
}

const (
	// the index is read from the first 8 bytes the generator produces
	lookupChoiceIndexSize = 8
	// transformer constructors receive no context, so the load carries its own
	// deadline rather than blocking startup indefinitely on a locked or
	// unreachable lookup table
	lookupChoiceLoadTimeout = 30 * time.Second

	randomGenerator        = "random"
	deterministicGenerator = "deterministic"
)

var (
	errLookupTableNotFound  = errors.New("lookup_choice: lookup_table must be provided")
	errLookupColumnNotFound = errors.New("lookup_choice: lookup_column must be provided")
	errLookupURLNotFound    = errors.New("lookup_choice: postgres_url must be provided")
	errLookupNoValues       = errors.New("lookup_choice: no values loaded")

	lookupChoiceCompatibleTypes = []SupportedDataType{
		StringDataType,
		CitextDataType,
		ByteArrayDataType,
		BooleanDataType,
		Integer16DataType,
		Integer32DataType,
		Integer64DataType,
		Float32DataType,
		Float64DataType,
		UInt8ArrayOf16DataType,
		DateDataType,
		DatetimeDataType,
	}

	lookupChoiceParams = []Parameter{
		{
			Name:          "lookup_table",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "lookup_column",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "postgres_url",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "generator",
			SupportedType: "string",
			Default:       "random",
			Dynamic:       false,
			Required:      false,
			Values:        []any{randomGenerator, deterministicGenerator},
		},
		{
			Name:          "ignore_values",
			SupportedType: "array",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
	}
)

// NewLookupChoiceTransformer reads the values of the configured lookup column
// and returns a transformer that chooses from them. The connection is only
// held for the duration of the read.
func NewLookupChoiceTransformer(params ParameterValues) (*LookupChoiceTransformer, error) {
	table, found, err := FindParameter[string](params, "lookup_table")
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: lookup_table must be a string: %w", err)
	}
	if !found || table == "" {
		return nil, errLookupTableNotFound
	}

	column, found, err := FindParameter[string](params, "lookup_column")
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: lookup_column must be a string: %w", err)
	}
	if !found || column == "" {
		return nil, errLookupColumnNotFound
	}

	url, found, err := FindParameter[string](params, "postgres_url")
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: postgres_url must be a string: %w", err)
	}
	// an empty URL would otherwise reach pgx, which falls back to the libpq
	// environment defaults and reads whichever database those point at
	if !found || url == "" {
		return nil, errLookupURLNotFound
	}

	ignoreValues, _, err := FindParameterArray[any](params, "ignore_values")
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: ignore_values must be an array: %w", err)
	}

	generatorType, err := FindParameterWithDefault(params, "generator", randomGenerator)
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: generator must be a string: %w", err)
	}
	// validated before the load so that a typo costs a config error rather
	// than a full table scan
	if generatorType != randomGenerator && generatorType != deterministicGenerator {
		return nil, fmt.Errorf("lookup_choice: generator must be one of 'random' or 'deterministic': %w", ErrInvalidParameters)
	}

	values, columnOID, err := loadLookupValues(url, table, column)
	if err != nil {
		return nil, err
	}

	compatibleTypes, err := lookupColumnTypes(columnOID, values[0])
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: column %s of table %s: %w", column, table, err)
	}

	// the replication path delivers a timestamp as the text wal2json emits
	// while a snapshot delivers pgx's time.Time, and the two cannot be
	// rendered to a common form reliably, so the same row would hash to
	// different values either side of the cutover
	if generatorType == deterministicGenerator && (columnOID == pgtype.TimestampOID || columnOID == pgtype.TimestamptzOID) {
		return nil, fmt.Errorf("lookup_choice: column %s of table %s is a timestamp, which the deterministic generator cannot key on consistently across snapshot and replication: %w",
			column, table, ErrInvalidParameters)
	}

	values, err = removeIgnoredValues(values, ignoreValues, columnOID == pgtype.DateOID)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("lookup_choice: every value in column %s of table %s is excluded by ignore_values", column, table)
	}

	return newLookupChoiceTransformer(values, generatorType, compatibleTypes, columnOID == pgtype.DateOID)
}

// newLookupChoiceTransformer builds the transformer around an already loaded
// list of values, so that the choosing logic can be tested without a database.
func newLookupChoiceTransformer(values []any, generatorType string, compatibleTypes []SupportedDataType, dateColumn bool) (*LookupChoiceTransformer, error) {
	if len(values) == 0 {
		return nil, errLookupNoValues
	}

	var generator generators.Generator
	var err error
	switch generatorType {
	case deterministicGenerator:
		generator, err = generators.NewDeterministicBytesGenerator(lookupChoiceIndexSize)
		if err != nil {
			return nil, fmt.Errorf("lookup_choice: error creating deterministic generator: %w", err)
		}
	case randomGenerator:
		generator = generators.NewRandomBytesGenerator(lookupChoiceIndexSize)
	default:
		return nil, fmt.Errorf("lookup_choice: generator must be one of 'random' or 'deterministic': %w", ErrInvalidParameters)
	}

	return &LookupChoiceTransformer{
		values:          values,
		generator:       generator,
		compatibleTypes: compatibleTypes,
		dateColumn:      dateColumn,
	}, nil
}

func (t *LookupChoiceTransformer) Transform(_ context.Context, value Value) (any, error) {
	index, err := t.generator.Generate([]byte(lookupValueKey(value.TransformValue, t.dateColumn)))
	if err != nil {
		return nil, fmt.Errorf("lookup_choice: generating value index: %w", err)
	}
	if len(index) < lookupChoiceIndexSize {
		return nil, fmt.Errorf("lookup_choice: generated index is %d bytes, expected %d", len(index), lookupChoiceIndexSize)
	}

	return t.values[binary.BigEndian.Uint64(index[:lookupChoiceIndexSize])%uint64(len(t.values))], nil
}

// CompatibleTypes reports the pg types the lookup column's values can be
// written to, so that a rule pointing a column at a lookup column of an
// incompatible type fails on startup rather than mid load.
func (t *LookupChoiceTransformer) CompatibleTypes() []SupportedDataType {
	return t.compatibleTypes
}

func (t *LookupChoiceTransformer) Type() TransformerType {
	return LookupChoice
}

func (t *LookupChoiceTransformer) IsDynamic() bool {
	return false
}

// Uniqueness is lossy in both generator modes: the values come from a set that
// is normally much smaller than the number of rows being transformed.
func (t *LookupChoiceTransformer) Uniqueness() Uniqueness {
	return UniquenessLossy
}

// Close is a no-op: the connection used to read the values is released as soon
// as the read completes.
func (t *LookupChoiceTransformer) Close() error {
	return nil
}

func LookupChoiceTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: lookupChoiceCompatibleTypes,
		Parameters:     lookupChoiceParams,
		Uniqueness:     UniquenessLossy,
	}
}

// loadLookupValues reads the lookup column, returning its values and the pg
// type OID Postgres reported for it.
func loadLookupValues(url, table, column string) ([]any, uint32, error) {
	qualifiedName, err := pglib.NewQualifiedName(table)
	if err != nil {
		return nil, 0, fmt.Errorf("lookup_choice: invalid lookup_table %q: %w", table, err)
	}
	schema := qualifiedName.Schema()
	if schema == "" {
		schema = "public"
	}

	// BuildFn receives no context, so the caller's cancellation cannot reach
	// this load; the deadline is what keeps a locked lookup table from
	// blocking startup with no way out but SIGKILL
	ctx, cancel := context.WithTimeout(context.Background(), lookupChoiceLoadTimeout)
	defer cancel()

	querier, err := lookup.NewQuerier(ctx, url)
	if err != nil {
		return nil, 0, fmt.Errorf("lookup_choice: creating connection pool: %w", err)
	}
	defer querier.Close(ctx)

	// the order is explicit because the deterministic generator picks an index
	// into this slice, and an unordered scan can return the rows differently on
	// every run
	quotedColumn := pglib.QuoteIdentifier(column)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s",
		quotedColumn, pglib.QuoteQualifiedIdentifier(schema, qualifiedName.Name()), quotedColumn, quotedColumn)

	rows, err := querier.Query(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("lookup_choice: querying column %s of table %s: %w", column, table, err)
	}
	defer rows.Close()

	var columnOID uint32
	if fields := rows.FieldDescriptions(); len(fields) > 0 {
		columnOID = fields[0].DataTypeOID
	}

	var values []any
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			return nil, 0, fmt.Errorf("lookup_choice: scanning column %s of table %s: %w", column, table, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("lookup_choice: reading column %s of table %s: %w", column, table, err)
	}
	if len(values) == 0 {
		return nil, 0, fmt.Errorf("lookup_choice: no values found in column %s of table %s", column, table)
	}

	return values, columnOID, nil
}

// removeIgnoredValues drops the configured values from the loaded list. An
// ignore value that matches nothing is an error: it is either a typo or a
// value written in a form the column never produces, and silently ignoring it
// leaves the excluded row in the choice set.
func removeIgnoredValues(values, ignoreValues []any, dateColumn bool) ([]any, error) {
	if len(ignoreValues) == 0 {
		return values, nil
	}

	// the ignored values come from the configuration and the lookup values
	// from the database, so they can represent the same value with different
	// Go types
	matched := make(map[string]bool, len(ignoreValues))
	for _, value := range ignoreValues {
		matched[lookupValueKey(value, dateColumn)] = false
	}

	kept := make([]any, 0, len(values))
	for _, value := range values {
		key := lookupValueKey(value, dateColumn)
		if _, found := matched[key]; found {
			matched[key] = true
			continue
		}
		kept = append(kept, value)
	}

	for key, found := range matched {
		if !found {
			return nil, fmt.Errorf("lookup_choice: ignore_values entry %q matches no value in the lookup column: %w", key, ErrInvalidParameters)
		}
	}

	return kept, nil
}

// lookupValueKey gives a value a canonical representation, used both to match
// ignore_values and to feed the generator. A snapshot delivers the values pgx
// decodes while replication delivers the text wal2json emits, so the two have
// to be rendered to the same string or the same row would be mapped
// differently either side of the cutover.
func lookupValueKey(value any, dateColumn bool) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case [16]byte:
		return encodeUUID(v)
	case time.Time:
		if dateColumn {
			return v.Format(time.DateOnly)
		}
		return v.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// encodeUUID renders the [16]byte pgx decodes a uuid into as the canonical
// text form replication delivers.
func encodeUUID(value [16]byte) string {
	buf := make([]byte, 36)
	hex.Encode(buf[0:8], value[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], value[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], value[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], value[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], value[10:16])
	return string(buf)
}

// lookupColumnTypes maps the pg type of the lookup column to the column types
// its values can be written to. Postgres reports the OID, which is exact;
// extension types have no fixed OID, so those fall back to the Go type pgx
// decoded. A type that matches neither is rejected, because silently
// accepting it would disable the compatibility check the parser relies on.
func lookupColumnTypes(columnOID uint32, sample any) ([]SupportedDataType, error) {
	switch columnOID {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return []SupportedDataType{StringDataType, CitextDataType}, nil
	case pgtype.BoolOID:
		return []SupportedDataType{BooleanDataType}, nil
	// a narrower integer or float is assignable to a wider column, which is
	// the ordinary shape of a foreign key referencing a serial primary key
	case pgtype.Int2OID:
		return []SupportedDataType{Integer16DataType, Integer32DataType, Integer64DataType}, nil
	case pgtype.Int4OID:
		return []SupportedDataType{Integer32DataType, Integer64DataType}, nil
	case pgtype.Int8OID:
		return []SupportedDataType{Integer64DataType}, nil
	case pgtype.Float4OID:
		return []SupportedDataType{Float32DataType, Float64DataType}, nil
	case pgtype.Float8OID:
		return []SupportedDataType{Float64DataType}, nil
	case pgtype.UUIDOID:
		return []SupportedDataType{UInt8ArrayOf16DataType}, nil
	case pgtype.ByteaOID:
		return []SupportedDataType{ByteArrayDataType}, nil
	case pgtype.DateOID:
		return []SupportedDataType{DateDataType}, nil
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return []SupportedDataType{DatetimeDataType}, nil
	}

	switch sample.(type) {
	case string:
		return []SupportedDataType{StringDataType, CitextDataType}, nil
	case []byte:
		return []SupportedDataType{ByteArrayDataType}, nil
	default:
		return nil, fmt.Errorf("unsupported lookup column type with OID %d: %w", columnOID, ErrInvalidParameters)
	}
}
