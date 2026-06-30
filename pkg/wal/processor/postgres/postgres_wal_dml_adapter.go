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
			needsTextCopy: needsTextCopy(types),
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
			val = a.updateValueForCopy(val, c.Type)
		}
		rowValues = append(rowValues, val)
	}
	return rowColumns, rowTypes, rowValues
}

func (a *dmlAdapter) updateValueForCopy(value any, colType string) any {
	// For COPY, we might need to update the value for some data types,
	// so that it will be able to be encoded into binary format correctly.
	switch colType {
	case "date", "timestamp", "timestamptz":
		return getInfinityValueForDateTime(value, colType)
	case "tstzrange":
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
		// If the value is a string (PostgreSQL array literal like "{val1,val2}"),
		// we need to parse it into a Go slice for binary COPY format
		if strVal, ok := value.(string); ok {
			// Use pgtype to parse the array string into a slice
			var arr pgtype.FlatArray[string]
			if err := a.pgTypeMap.SQLScanner(&arr).Scan(strVal); err == nil {
				return []string(arr)
			}
			// If parsing fails, return the original value and let pgx handle it
		}
	}

	return value
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
	case "int4range":
		return getTypedInt4Range(value)
	case "int8range":
		return getTypedInt8Range(value)
	case "tstzrange":
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

// textOnlyCopyTypes lists postgres type names whose binary wire format pgx
// can't produce correctly, so bulk ingest must fall back to text-format
// COPY for any batch that touches one of these columns.
var textOnlyCopyTypes = map[string]struct{}{
	"cube":  {}, // binary header: int32 dim+flags + N×float8 — pgx writes the text rep, server misreads it as a dimension count
	"ltree": {}, // binary format: 1-byte version + path string — pgx writes the text rep, server reads byte 0 as the version number
}

func needsTextCopy(columnTypes []string) bool {
	for _, t := range columnTypes {
		if _, ok := textOnlyCopyTypes[t]; ok {
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
