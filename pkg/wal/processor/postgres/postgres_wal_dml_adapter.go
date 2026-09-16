// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

type onConflictAction uint

const (
	onConflictError onConflictAction = iota
	onConflictUpdate
	onConflictDoNothing
)

const (
	int4rangeType = "int4range"
	int8rangeType = "int8range"
	tstzrangeType = "tstzrange"
)

var (
	errUnsupportedOnConflictAction = errors.New("unsupported on conflict action")
	errUnableToBuildQuery          = errors.New("unable to build query, no primary keys of previous values available")
)

type dmlAdapter struct {
	logger           loglib.Logger
	onConflictAction onConflictAction
	forCopy          bool
	pgTypeMap        *pgtype.Map
}

func newDMLAdapter(action string, forCopy bool, logger loglib.Logger) (*dmlAdapter, error) {
	oca, err := parseOnConflictAction(action)
	if err != nil {
		return nil, err
	}
	return &dmlAdapter{
		logger:           logger,
		onConflictAction: oca,
		forCopy:          forCopy,
		pgTypeMap:        pgtype.NewMap(),
	}, nil
}

func (a *dmlAdapter) walDataToQueries(d *wal.Data, schemaInfo schemaInfo) ([]*query, error) {
	switch d.Action {
	case "T":
		return []*query{a.buildTruncateQuery(d)}, nil
	case "D":
		q, err := a.buildDeleteQuery(d)
		if err != nil {
			return nil, err
		}
		return []*query{q}, nil
	case "I":
		return a.buildInsertQueries(d, schemaInfo), nil
	case "U":
		q, err := a.buildUpdateQuery(d, schemaInfo)
		if err != nil {
			return nil, err
		}
		return []*query{q}, nil
	default:
		return []*query{}, nil
	}
}

func (a *dmlAdapter) buildTruncateQuery(d *wal.Data) *query {
	return &query{
		table:  d.Table,
		schema: d.Schema,
		sql:    fmt.Sprintf("TRUNCATE %s", quotedTableName(d.Schema, d.Table)),
	}
}

func (a *dmlAdapter) buildDeleteQuery(d *wal.Data) (*query, error) {
	whereQuery, whereValues, err := a.buildWhereQuery(d, 0)
	if err != nil {
		return nil, fmt.Errorf("building delete query: %w", err)
	}
	return &query{
		table:  d.Table,
		schema: d.Schema,
		sql:    fmt.Sprintf("DELETE FROM %s %s", quotedTableName(d.Schema, d.Table), whereQuery),
		args:   whereValues,
	}, nil
}

func (a *dmlAdapter) buildInsertQueries(d *wal.Data, schemaInfo schemaInfo) []*query {
	names, types, values := a.filterRowColumnsWithTypes(d.Columns, schemaInfo)
	// if there are no columns after filtering generated ones, no query to run
	if len(names) == 0 {
		return []*query{}
	}

	placeholders := make([]string, 0, len(d.Columns))
	for i := range names {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	qs := []*query{
		{
			table:         d.Table,
			schema:        d.Schema,
			columnNames:   names,
			needsTextCopy: a.needsTextCopyForColumns(names, types, schemaInfo.enumColumns),
			sql: fmt.Sprintf("INSERT INTO %s(%s) OVERRIDING SYSTEM VALUE VALUES(%s)%s",
				quotedTableName(d.Schema, d.Table), strings.Join(names, ", "),
				strings.Join(placeholders, ", "),
				a.buildOnConflictQuery(d, names)),
			args: values,
		},
	}

	// for COPY we don't need to handle sequence updates
	if a.forCopy {
		return qs
	}

	// handle sequence columns that need to be updated after insert
	for _, col := range d.Columns {
		if seqName, ok := schemaInfo.sequenceColumns[pglib.QuoteIdentifier(col.Name)]; ok {
			seqVal, ok := toInt64(col.Value)
			if !ok {
				a.logger.Warn(nil, "unexpected value type for sequence column, expected integer", loglib.Fields{
					"column_name": col.Name, "column_type": col.Type, "column_value": col.Value,
				})
				continue
			}
			qs = append(qs, &query{
				table:  d.Table,
				schema: d.Schema,
				sql:    "SELECT setval($1::regclass, $2::bigint, true)",
				args:   []any{seqName, seqVal},
			})
		}
	}

	return qs
}

func (a *dmlAdapter) buildUpdateQuery(d *wal.Data, schemaInfo schemaInfo) (*query, error) {
	rowColumns, _, rowValues := a.filterRowColumnsForAction(d.Columns, schemaInfo, true)
	// if there are no columns after filtering generated ones, no query to run
	if len(rowColumns) == 0 {
		return &query{}, nil
	}

	setQuery, setValues := a.buildSetQuery(d.Columns, rowColumns, rowValues)
	// if there are no columns after filtering generated ones, no query to run
	if setQuery == "" {
		return &query{}, nil
	}
	whereQuery, whereValues, err := a.buildWhereQuery(d, len(rowColumns))
	if err != nil {
		return nil, fmt.Errorf("building update query: %w", err)
	}

	return &query{
		table:  d.Table,
		schema: d.Schema,
		sql:    fmt.Sprintf("UPDATE %s %s %s", quotedTableName(d.Schema, d.Table), setQuery, whereQuery),
		args:   append(setValues, whereValues...),
	}, nil
}

func (a *dmlAdapter) buildWhereQuery(d *wal.Data, placeholderOffset int) (string, []any, error) {
	var cols []wal.Column
	switch {
	case len(d.Identity) > 0:
		// if we have the previous values (replica identity), add them to the where query
		cols = d.Identity
	case len(d.Metadata.InternalColIDs) > 0:
		// if we don't have previous values we have to rely on the primary keys
		primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Columns)
		cols = primaryKeyCols
	default:
		// without a where clause in the query we'd be updating/deleting all table
		// rows, so we need to error to prevent that from happening
		return "", nil, errUnableToBuildQuery
	}

	whereQuery := "WHERE"
	whereValues := make([]any, 0, len(cols))
	placeholderIdx := placeholderOffset
	for i, c := range cols {
		if i != 0 {
			whereQuery = fmt.Sprintf("%s AND", whereQuery)
		}

		if c.Value == nil {
			whereQuery = fmt.Sprintf("%s %s IS NULL", whereQuery, pglib.QuoteIdentifier(c.Name))
			continue
		}

		placeholderIdx++
		whereQuery = fmt.Sprintf("%s %s = $%d", whereQuery, pglib.QuoteIdentifier(c.Name), placeholderIdx)
		whereValues = append(whereValues, serializeJSONBValue(c.Type, c.Value))

	}
	return whereQuery, whereValues, nil
}

func (a *dmlAdapter) buildSetQuery(cols []wal.Column, rowColumns []string, rowValues []any) (string, []any) {
	setQuery := "SET"
	setValues := make([]any, 0, len(cols))
	for i, column := range rowColumns {
		if i != 0 {
			setQuery = fmt.Sprintf("%s,", setQuery)
		}
		setQuery = fmt.Sprintf("%s %s = $%d", setQuery, column, i+1)
		setValues = append(setValues, rowValues[i])
	}
	return setQuery, setValues
}

func (a *dmlAdapter) buildOnConflictQuery(d *wal.Data, filteredColumnNames []string) string {
	switch a.onConflictAction {
	case onConflictUpdate:
		// on conflict do update requires a conflict target. If there are no
		// primary keys to use for the conflict target, default to error
		// behaviour
		primaryKeyCols := a.extractPrimaryKeyColumnNames(d.Metadata.InternalColIDs, d.Columns)
		if len(primaryKeyCols) == 0 {
			return ""
		}

		cols := make([]string, 0, len(d.Columns))
		for _, col := range filteredColumnNames {
			cols = append(cols, fmt.Sprintf("%[1]s = EXCLUDED.%[1]s", col))
		}
		return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(primaryKeyCols, ","), strings.Join(cols, ", "))
	case onConflictDoNothing:
		return " ON CONFLICT DO NOTHING"
	default:
		return ""
	}
}

func (a *dmlAdapter) extractPrimaryKeyColumns(colIDs []string, cols []wal.Column) []wal.Column {
	primaryKeyColumns := make([]wal.Column, 0, len(colIDs))
	for _, col := range cols {
		if !slices.Contains(colIDs, col.ID) {
			continue
		}
		primaryKeyColumns = append(primaryKeyColumns, col)
	}

	return primaryKeyColumns
}

func (a *dmlAdapter) extractPrimaryKeyColumnNames(colIDs []string, cols []wal.Column) []string {
	primaryKeyCols := a.extractPrimaryKeyColumns(colIDs, cols)
	if len(primaryKeyCols) == 0 {
		return []string{}
	}
	colNames := []string{}
	for _, col := range primaryKeyCols {
		colNames = append(colNames, pglib.QuoteIdentifier(col.Name))
	}
	return colNames
}

func (a *dmlAdapter) filterRowColumns(cols []wal.Column, schemaInfo schemaInfo) ([]string, []any) {
	names, _, vals := a.filterRowColumnsForAction(cols, schemaInfo, false)
	return names, vals
}

// filterRowColumnsWithTypes is the variant used on the bulk-COPY path: it
// also returns the postgres type name for each kept column so the writer
// can decide between binary and text-format COPY.
func (a *dmlAdapter) filterRowColumnsWithTypes(cols []wal.Column, schemaInfo schemaInfo) ([]string, []string, []any) {
	return a.filterRowColumnsForAction(cols, schemaInfo, false)
}

// filterRowColumnsForAction drops generated columns, and — when forUpdate is
// true — also drops GENERATED ALWAYS AS IDENTITY columns. INSERTs use
// OVERRIDING SYSTEM VALUE so always-identity values are accepted, but no such
// clause exists for UPDATE and Postgres rejects explicit values in SET.
func (a *dmlAdapter) filterRowColumnsForAction(cols []wal.Column, schemaInfo schemaInfo, forUpdate bool) ([]string, []string, []any) {
	rowValues := make([]any, 0, len(cols))
	rowColumns := make([]string, 0, len(cols))
	rowTypes := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted := pglib.QuoteIdentifier(c.Name)
		if _, found := schemaInfo.generatedColumns[quoted]; found {
			continue
		}
		if forUpdate {
			if _, found := schemaInfo.alwaysIdentityColumns[quoted]; found {
				continue
			}
		}
		rowColumns = append(rowColumns, quoted)
		rowTypes = append(rowTypes, c.Type)
		val := c.Value

		val = serializeJSONBValue(c.Type, val)
		val = getTypedRangeValue(c.Type, val)

		if a.forCopy {
			_, isEnum := schemaInfo.enumColumns[quoted]
			val = a.updateValueForCopy(val, c.Type, isEnum || a.needsTextCopyForType(c.Type))
		}
		rowValues = append(rowValues, val)
	}
	return rowColumns, rowTypes, rowValues
}

// updateValueForCopy adapts a value so the COPY encoder can render it.
// textCopy reports whether the column's own type forces the batch onto
// text-format COPY (see needsTextCopyForColumns), which changes what the
// encoder can accept for an array value.
func (a *dmlAdapter) updateValueForCopy(value any, colType string, textCopy bool) any {
	// For COPY, we might need to update the value for some data types,
	// so that it will be able to be encoded into binary format correctly.

	// A transformer can turn a range into its postgres literal. pgx has no
	// binary encode plan from a string to a range, so parse it back into a
	// typed range first.
	if strVal, ok := value.(string); ok {
		if typed, ok := a.parseRangeLiteral(colType, strVal); ok {
			return typed
		}
	}

	switch colType {
	case "date", "timestamp", "timestamptz":
		return getInfinityValueForDateTime(value, colType)
	case tstzrangeType:
		return getTypedTSTZRange(value)
	case "tsvector":
		if b, ok := value.([]byte); ok {
			return string(b)
		}
		return value
	}

	// Handle array types
	// For COPY binary format, array values that come as PostgreSQL text literals (strings)
	// need to be converted to Go slices. The pgx COPY encoder expects proper Go types,
	// not text representations.
	if isArray(colType) {
		// An array of a user-defined enum, or of any other type pgx has no
		// binary codec for, never reaches binary COPY: the batch is routed to
		// text-format COPY, which writes the postgres array literal verbatim.
		// Parsing it into a Go slice here would hand the text encoder a type it
		// cannot render back.
		if textCopy {
			return value
		}
		// If the value is a string (PostgreSQL array literal like "{val1,val2}"),
		// we need to parse it into a Go slice for binary COPY format
		if strVal, ok := value.(string); ok {
			// pgtype.Array keeps the dimensions, pgtype.FlatArray does not.
			// With a flat array, {{1,2},{3,4}} reaches the target as
			// {1,2,3,4}: the shape is lost, no error is raised, and nothing
			// counts it.
			//
			// The pgx value stays inside this sink. What arrives here is the
			// postgres array literal, which is what every other target sees.
			var arr pgtype.Array[string]
			if err := a.pgTypeMap.SQLScanner(&arr).Scan(strVal); err == nil {
				if len(arr.Dims) > 1 {
					return arr
				}
				return arr.Elements
			}
			// If parsing fails, return the original value and let pgx handle it
		}
	}

	return value
}

// parseRangeLiteral scans a postgres range literal into the typed range pgx
// can encode in binary format for the column type. It reports false for a
// column that is not a range, or a literal that does not parse.
func (a *dmlAdapter) parseRangeLiteral(colType, literal string) (any, bool) {
	switch colType {
	case int4rangeType:
		return scanRangeLiteral[int32](a.pgTypeMap, pgtype.Int4rangeOID, literal)
	case int8rangeType:
		return scanRangeLiteral[int64](a.pgTypeMap, pgtype.Int8rangeOID, literal)
	case "numrange":
		return scanRangeLiteral[pgtype.Numeric](a.pgTypeMap, pgtype.NumrangeOID, literal)
	case "daterange":
		return scanRangeLiteral[pgtype.Date](a.pgTypeMap, pgtype.DaterangeOID, literal)
	case "tsrange":
		return scanRangeLiteral[pgtype.Timestamp](a.pgTypeMap, pgtype.TsrangeOID, literal)
	case tstzrangeType:
		return scanRangeLiteral[time.Time](a.pgTypeMap, pgtype.TstzrangeOID, literal)
	default:
		return nil, false
	}
}

func scanRangeLiteral[T any](typeMap *pgtype.Map, oid uint32, literal string) (any, bool) {
	var r pgtype.Range[T]
	if err := typeMap.Scan(oid, pgtype.TextFormatCode, []byte(literal), &r); err != nil {
		return nil, false
	}
	return r, true
}

func quotedTableName(schemaName, tableName string) string {
	return pglib.QuoteQualifiedIdentifier(schemaName, tableName)
}

func parseOnConflictAction(action string) (onConflictAction, error) {
	switch action {
	case "", "error":
		return onConflictError, nil
	case "update":
		return onConflictUpdate, nil
	case "nothing":
		return onConflictDoNothing, nil
	default:
		return 0, errUnsupportedOnConflictAction
	}
}

func getInfinityValueForDateTime(value any, colType string) any {
	v, ok := value.(pgtype.InfinityModifier)
	if !ok {
		// If not infinity, just return the value as is
		return value
	}

	switch colType {
	case "date":
		return pgtype.Date{Valid: true, InfinityModifier: v}
	case "timestamp":
		return pgtype.Timestamp{Valid: true, InfinityModifier: v}
	case "timestamptz":
		return pgtype.Timestamptz{Valid: true, InfinityModifier: v}
	}
	return value
}

func getTypedTSTZRange(value any) any {
	v, ok := value.(pgtype.Range[any])
	if !ok {
		return value
	}

	lower, lowerOk := v.Lower.(time.Time)
	if !lowerOk {
		lower = time.Time{}
	}

	upper, upperOk := v.Upper.(time.Time)
	if !upperOk {
		upper = time.Time{}
	}

	return pgtype.Range[time.Time]{
		Lower:     lower,
		Upper:     upper,
		LowerType: v.LowerType,
		UpperType: v.UpperType,
		Valid:     v.Valid,
	}
}

func getTypedRangeValue(colType string, value any) any {
	switch colType {
	case int4rangeType:
		return getTypedInt4Range(value)
	case int8rangeType:
		return getTypedInt8Range(value)
	case tstzrangeType:
		return getTypedTSTZRange(value)
	default:
		return value
	}
}

func getTypedInt4Range(value any) any {
	v, ok := value.(pgtype.Range[any])
	if !ok {
		return value
	}

	lower, lowerOk := toInt64(v.Lower)
	upper, upperOk := toInt64(v.Upper)

	var typedLower, typedUpper int32
	if lowerOk {
		typedLower = int32(lower)
	}
	if upperOk {
		typedUpper = int32(upper)
	}

	return pgtype.Range[int32]{
		Lower:     typedLower,
		Upper:     typedUpper,
		LowerType: v.LowerType,
		UpperType: v.UpperType,
		Valid:     v.Valid,
	}
}

func getTypedInt8Range(value any) any {
	v, ok := value.(pgtype.Range[any])
	if !ok {
		return value
	}

	lower, lowerOk := toInt64(v.Lower)
	upper, upperOk := toInt64(v.Upper)

	var typedLower, typedUpper int64
	if lowerOk {
		typedLower = lower
	}
	if upperOk {
		typedUpper = upper
	}

	return pgtype.Range[int64]{
		Lower:     typedLower,
		Upper:     typedUpper,
		LowerType: v.LowerType,
		UpperType: v.UpperType,
		Valid:     v.Valid,
	}
}

// textOnlyCopyTypes holds the extension types pgstream registers a text-only
// codec for (see internal/postgres extensionTypes): pgx knows the name, and
// will happily ask the codec for the binary format, but what it gets back is
// the text representation. Bulk ingest must fall back to text-format COPY for
// any batch that touches one of these columns.
var textOnlyCopyTypes = typeNameSet(pglib.TextCopyOnlyTypeNames())

// binaryCopySafeTypes holds the extension types pgstream registers a
// binary-capable codec for (hstore, the pgvector family). pgx's static type
// map has no entry for them, so the unknown-type rule in needsTextCopyForType
// would otherwise drag their batches onto the slower text-format COPY.
var binaryCopySafeTypes = typeNameSet(pglib.BinaryCopySafeTypeNames())

func typeNameSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return set
}

// pgTypeName normalises a type name to the name pgx registers it under:
// an array spelled with [] becomes the _element form pgx uses.
func pgTypeName(colType string) string {
	if element, isArray := strings.CutSuffix(colType, "[]"); isArray {
		return "_" + pgTypeName(element)
	}
	return colType
}

// needsTextCopyForType reports whether a column of the given postgres type must
// be written with text-format COPY instead of pgx's binary COPY.
//
// pgx can only produce the binary wire format for the types in its static type
// map, plus the extension types pgstream registers a binary-capable codec for.
// Every other type — a PostGIS geometry or geography, citext, money, a
// composite, a user-defined enum — is asked for in text format when the
// snapshot reads it, because pgx has no codec for the OID, so it reaches the
// writer as its text representation. Binary COPY would send those text bytes
// as if they were the type's binary layout, and the server misreads them: for
// geometry, postgres reads the first character of the hex EWKB as the byte
// order flag and fails with "Invalid endian flag value encountered".
func (a *dmlAdapter) needsTextCopyForType(colType string) bool {
	// A column with no type name keeps the binary COPY path. The bulk path is
	// fed by the snapshot, which resolves a name for every column, so an empty
	// one means the caller had none to give rather than that the type is one
	// pgx cannot encode.
	if colType == "" {
		return false
	}

	name := pgTypeName(colType)
	if _, textOnly := textOnlyCopyTypes[name]; textOnly {
		return true
	}
	if _, binarySafe := binaryCopySafeTypes[name]; binarySafe {
		return false
	}
	_, knownToPgx := a.pgTypeMap.TypeForName(name)
	return !knownToPgx
}

func (a *dmlAdapter) needsTextCopy(columnTypes []string) bool {
	for _, t := range columnTypes {
		if a.needsTextCopyForType(t) {
			return true
		}
	}
	return false
}

// needsTextCopyForColumns reports whether a batch covering the given columns
// must fall back to text-format COPY instead of pgx's binary COPY. This is the
// case when a column has a type pgx cannot encode in binary format (see
// needsTextCopyForType) or a user-defined enum type, whose database-specific
// OID pgx has no binary codec registered for. columnNames must be quoted to
// match the enumColumns set.
func (a *dmlAdapter) needsTextCopyForColumns(columnNames, columnTypes []string, enumColumns map[string]enumColumn) bool {
	if a.needsTextCopy(columnTypes) {
		return true
	}
	for _, name := range columnNames {
		if _, ok := enumColumns[name]; ok {
			return true
		}
	}
	return false
}

// toInt64 converts a wal.Column.Value into an int64 if it represents an
// integer. WAL data deserialised with UseInt64 produces int64, but snapshots
// and tests may produce other integer types or float64.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

func isArray(colType string) bool {
	// PostgreSQL array types can be represented in two ways:
	// 1. With [] suffix: text[], int[], etc.
	// 2. With _ prefix: _text, _int4, _ExampleEnum, etc. (internal representation)
	return (len(colType) > 2 && colType[len(colType)-2:] == "[]") ||
		(len(colType) > 1 && colType[0] == '_')
}

// serializeJSONBValue pre-serializes JSONB/JSON values to ensure consistent
// encoding between Sonic (wal2json parsing) and pgx (encoding/json).
// Map and slice values are always serialized. String values are serialized
// only if they are not already valid JSON (e.g. JSON scalar strings like
// "FIRST" from pgx rows.Values()), to avoid double-encoding pre-built JSON
// documents passed as strings (e.g. from the schemalog snapshot generator).
func serializeJSONBValue(colType string, val any) any {
	if (colType == "jsonb" || colType == "json") && val != nil {
		switch v := val.(type) {
		case map[string]any, []any:
			if jsonBytes, err := json.Marshal(val); err == nil {
				return jsonBytes
			}
		case string:
			if !stdjson.Valid([]byte(v)) {
				if jsonBytes, err := json.Marshal(v); err == nil {
					return jsonBytes
				}
			}
		}
	}
	return val
}
