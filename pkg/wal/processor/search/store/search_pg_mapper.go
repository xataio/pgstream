// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/internal/searchstore"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type PgMapper struct {
	searchMapper searchstore.Mapper
	pgTypeMap    *pgtype.Map
	unmarshaler  func([]byte, any) error
}

const (
	// Default date_time pattern
	timestampTZFormat = "2006-01-02T15:04:05.000Z"
	timestampFormat   = "2006-01-02T15:04:05.000"
	dateFormat        = "2006-01-02"
)

// NewPostgresMapper returns a mapper that maps between postgres and search
// store types
func NewPostgresMapper(mapper searchstore.Mapper) *PgMapper {
	return &PgMapper{
		searchMapper: mapper,
		pgTypeMap:    pgtype.NewMap(),
		unmarshaler:  json.Unmarshal,
	}
}

// ColumnToSearchMapping maps the column on input into the equivalent search mapping
func (m *PgMapper) ColumnToSearchMapping(column schemalog.Column) (map[string]any, error) {
	searchField, err := m.columnToSearchField(column)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pg type (%s): %w", column.DataType, err)
	}

	return m.searchMapper.FieldMapping(searchField)
}

// MapColumnValue maps a value emitted from PG into a value that the search
// store can handle. If the column is a timestamp: we need to parse it. If the
// column is an array of any type except json, we need to map it to a Go slice.
// If column type is unknown we return nil. This avoids dropping the whole
// record if one field type is unknown.
func (m *PgMapper) MapColumnValue(column schemalog.Column, value any) (any, error) {
	searchField, err := m.columnToSearchField(column)
	if err != nil {
		return nil, fmt.Errorf("mapping column from pg to search store: %w", err)
	}

	if value == nil {
		return nil, nil
	}

	switch searchField.SearchType {
	case searchstore.DateTimeTZType, searchstore.DateTimeType:
		if searchField.IsArray {
			return m.mapDateTimeArray(searchField, value)
		} else {
			return m.mapDateTime(searchField, value)
		}
	case searchstore.DateType:
		var d pgtype.Date
		if err := d.Scan(value); err != nil {
			return nil, fmt.Errorf("mapping date from pg to search store failed: %w (value: %s)", err, value)
		}
		return d.Time.Format(dateFormat), nil
	case searchstore.PGVectorType:
		// pgvector vectors come as strings. We need to parse them into arrays of floats.
		stringContent, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for jsonb column")
		}
		var array []float64
		err := m.unmarshaler([]byte(stringContent), &array)
		if err != nil {
			return nil, fmt.Errorf("vector value is not array: %w", err)
		}
		return array, nil
	default:
		if searchField.IsArray { // catches all other array types
			// handle arrays
			switch searchField.SearchType {
			case searchstore.IntegerType:
				var a pgtype.FlatArray[int64]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []int64(a), err
			case searchstore.FloatType:
				var a pgtype.FlatArray[float64]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []float64(a), err
			case searchstore.BoolType:
				var a pgtype.FlatArray[bool]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []bool(a), err
			case searchstore.StringType:
				var a pgtype.FlatArray[string]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []string(a), err
			case searchstore.JSONType:
				// nothing to do for json array types
			default:
				// should never get here
				panic(fmt.Sprintf("indexer: unexpected array type: %v", searchField.SearchType))
			}
		}
	}

	// otherwise: do nothing, return the original value
	return value, nil
}

func (m *PgMapper) columnToSearchField(column schemalog.Column) (*searchstore.Field, error) {
	pgTypeName := column.DataType
	typeName, isArray, err := m.parsePGType(pgTypeName)
	if err != nil {
		return nil, fmt.Errorf("pg to search type: failed to parse pg type: %w", err)
	}

	metadata := searchstore.Metadata{}

	var searchType searchstore.Type
	switch typeName {
	case "int8", "int2", "int4", "integer", "smallint", "bigint":
		searchType = searchstore.IntegerType
	case "float4", "float8", "real", "double precision", "float", "numeric":
		searchType = searchstore.FloatType
	case "boolean":
		searchType = searchstore.BoolType
	case "bytea", "char", "name", "text", "varchar", "bpchar", "xml", "uuid", "character varying", "character", "cidr", "inet", "macaddr", "macaddr8", "interval":
		searchType = searchstore.StringType
	case "jsonb", "json":
		searchType = searchstore.JSONType
	case "date":
		searchType = searchstore.DateType
	case "time", "time with time zone", "time without time zone":
		searchType = searchstore.TimeType
	case "timestamp", "timestamp without time zone":
		searchType = searchstore.DateTimeType
	case "timestamptz", "timetz", "timestamp with time zone":
		searchType = searchstore.DateTimeTZType
	default:
		// pgvector includes the schema (sometimes? seems only a problem when testing locally)
		if isPGVector(typeName) {
			searchType = searchstore.PGVectorType
			metadata.VectorDimension, err = getPGVectorDimension(typeName)
			if err != nil {
				return nil, search.ErrTypeInvalid{Input: pgTypeName}
			}
		} else {
			return nil, search.ErrTypeInvalid{Input: pgTypeName}
		}
	}

	return &searchstore.Field{
		SearchType: searchType,
		IsArray:    isArray,
		Metadata:   metadata,
	}, nil
}

func (m *PgMapper) mapDateTimeArray(searchField *searchstore.Field, value any) (any, error) {
	switch searchField.SearchType {
	case searchstore.DateTimeTZType:
		var a pgtype.FlatArray[pgtype.Timestamptz]
		err := m.pgTypeMap.SQLScanner(&a).Scan(value)
		if err != nil {
			return nil, fmt.Errorf("mapping timestamptz array from pg to search store failed: %w (value: %s)", err, value)
		}

		dts := make([]string, len(a))

		for i := range a {
			dts[i] = a[i].Time.Truncate(time.Millisecond).Format(timestampTZFormat)
		}

		return dts, nil
	case searchstore.DateTimeType:
		var a pgtype.FlatArray[pgtype.Timestamp]
		err := m.pgTypeMap.SQLScanner(&a).Scan(value)
		if err != nil {
			return nil, fmt.Errorf("mapping timestampt array from pg to search store failed: %w (value: %s)", err, value)
		}

		dts := make([]string, len(a))

		for i := range a {
			dts[i] = a[i].Time.Truncate(time.Millisecond).Format(timestampFormat)
		}

		return dts, nil
	}
	return value, nil
}

func (m *PgMapper) mapDateTime(searchField *searchstore.Field, value any) (any, error) {
	switch searchField.SearchType {
	case searchstore.DateTimeTZType:
		var ts pgtype.Timestamptz
		if err := ts.Scan(value); err != nil {
			return nil, fmt.Errorf("mapping timestamptz from pg to search store failed: %w (value: %s)", err, value)
		}
		return ts.Time.Truncate(time.Millisecond).Format(timestampTZFormat), nil
	case searchstore.DateTimeType:
		var ts pgtype.Timestamp
		if err := ts.Scan(value); err != nil {
			return nil, fmt.Errorf("mapping timestamp from pg to search store failed: %w (value: %s)", err, value)
		}
		return ts.Time.Truncate(time.Millisecond).Format(timestampFormat), nil
	}
	return value, nil
}

func (m *PgMapper) parsePGType(name string) (typeName string, isArray bool, err error) {
	inputName := name

	if strings.HasSuffix(name, "[]") { // detect and strip array suffix. this is always last.
		isArray = true
		name = name[:len(name)-2]
	}

	if strings.HasSuffix(name, ")") { // detect and strip parameters suffix. this is always last.
		openingBracketIndex := strings.LastIndex(name, "(")
		if openingBracketIndex == -1 {
			return "", false, search.ErrTypeInvalid{Input: inputName}
		}
		name = name[:openingBracketIndex]
	}

	return name, isArray, nil
}

func isPGVector(colType string) bool {
	// pgvector includes the schema (sometimes? seems only a problem when
	// testing locally), make sure we remove it before checking for the type
	parts := strings.Split(colType, ".")
	if len(parts) > 1 {
		colType = parts[1]
	}
	return strings.HasPrefix(colType, "vector(")
}

func getPGVectorDimension(colType string) (int, error) {
	dimensionStr := strings.TrimSuffix(strings.TrimPrefix(colType, "vector("), ")")
	return strconv.Atoi(dimensionStr)
}
