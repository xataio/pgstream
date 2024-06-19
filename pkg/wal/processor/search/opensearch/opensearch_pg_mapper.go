// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type Mapper struct {
	pgTypeMap *pgtype.Map
}

type searchType uint

const (
	searchTypeInteger searchType = iota
	searchTypeFloat
	searchTypeBool
	searchTypeString
	searchTypeDateTimeTZ
	searchTypeDateTime
	searchTypeDate
	searchTypeTime
	searchTypeJSON
	searchTypeText
	searchTypePGVector
)

type searchField struct {
	searchType searchType
	isArray    bool
	metadata   metadata
}

type metadata struct {
	vectorDimension int
}

const (
	// Lucene’s term byte-length limit is 32766. To cover for the use of UTF-8
	// text with many non-ASCII characters, the maximum value should be 32766 /
	// 4 = 8191 since UTF-8 characters may occupy at most 4 bytes.
	termByteLengthLimit = 32766

	// Default ES date_time pattern
	timestampTZFormat = "2006-01-02T15:04:05.000Z"
	timestampFormat   = "2006-01-02T15:04:05.000"
	dateFormat        = "2006-01-02"
)

// NewPostgresMapper returns a mapper that maps between postgres and opensearch
// types
func NewPostgresMapper() *Mapper {
	return &Mapper{
		pgTypeMap: pgtype.NewMap(),
	}
}

// ColumnToSearchMapping maps the column on input into the equivalent search mapping
func (m *Mapper) ColumnToSearchMapping(column schemalog.Column) (map[string]any, error) {
	searchField, err := m.columnToSearchField(column)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pg type (%s): %w", column.DataType, err)
	}

	switch searchField.searchType {
	case searchTypeInteger:
		return map[string]any{"type": "long"}, nil
	case searchTypeFloat:
		return map[string]any{"type": "double"}, nil
	case searchTypeBool:
		return map[string]any{"type": "boolean"}, nil
	case searchTypeText, searchTypeJSON:
		return map[string]any{"type": "text"}, nil
	case searchTypeString:
		return map[string]any{
			"type":         "keyword",
			"ignore_above": termByteLengthLimit,
			"fields": map[string]any{
				"text": map[string]any{
					"type": "text",
				},
			},
		}, nil
	case searchTypeTime:
		return map[string]any{
			"type":   "date",
			"format": "HH:mm:ss[.SS][x][Z]||HH:mm:ss[.SSS][x][Z]||HH:mm:ss[.SSSSSS][x][Z]",
		}, nil
	case searchTypeDate:
		return map[string]any{
			"type":   "date",
			"format": "date",
		}, nil
	case searchTypeDateTime, searchTypeDateTimeTZ:
		return map[string]any{
			"type":   "date",
			"format": "yyyy-MM-dd HH:mm:ss[.SSS][x]||yyyy-MM-dd HH:mm:ss[.SS][x]||yyyy-MM-dd HH:mm:ss[.S][x]||yyyy-MM-dd'T'HH:mm:ss[.SSS][X]",
		}, nil
	case searchTypePGVector:
		vectorSettings := map[string]any{
			"type":      "knn_vector",
			"dimension": searchField.metadata.vectorDimension,
		}
		return vectorSettings, nil
	default:
		return nil, err
	}
}

// MapColumnValue maps a value emitted from PG into a value that OS can handle.
// If the column is a timestamp: we need to parse it.
// If the column is an array of any type except json, we need to map it to a Go slice.
// If column type is unknown we return nil. This avoids dropping the whole record if one field type is unknown.
func (m *Mapper) MapColumnValue(column schemalog.Column, value any) (any, error) {
	searchField, err := m.columnToSearchField(column)
	if err != nil {
		return nil, fmt.Errorf("mapping column from pg to os: %w", err)
	}

	if value == nil {
		return nil, nil
	}

	switch searchField.searchType {
	case searchTypeDateTimeTZ, searchTypeDateTime:
		if searchField.isArray {
			return m.mapDateTimeArray(searchField, value)
		} else {
			return m.mapDateTime(searchField, value)
		}
	case searchTypeDate:
		var d pgtype.Date
		if err := d.Scan(value); err != nil {
			return nil, fmt.Errorf("mapping date from pg to ES failed: %w (value: %s)", err, value)
		}
		return d.Time.Format(dateFormat), nil
	case searchTypePGVector:
		// pgvector vectors come as strings. We need to parse them into arrays of floats.
		stringContent, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for jsonb column")
		}
		var array []float64
		err := json.Unmarshal([]byte(stringContent), &array)
		if err != nil {
			return nil, fmt.Errorf("vector value is not array: %w", err)
		}
		return array, nil
	default:
		if searchField.isArray { // catches all other array types
			// handle arrays
			switch searchField.searchType {
			case searchTypeInteger:
				var a pgtype.FlatArray[int64]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []int64(a), err
			case searchTypeFloat:
				var a pgtype.FlatArray[float64]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []float64(a), err
			case searchTypeBool:
				var a pgtype.FlatArray[bool]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []bool(a), err
			case searchTypeString:
				var a pgtype.FlatArray[string]
				err := m.pgTypeMap.SQLScanner(&a).Scan(value)
				return []string(a), err
			case searchTypeJSON:
				// nothing to do for json array types
			default:
				// should never get here
				panic(fmt.Sprintf("indexer: unexpected array type: %v", searchField.searchType))
			}
		}
	}

	// otherwise: do nothing, return the original value
	return value, nil
}

func (m *Mapper) columnToSearchField(column schemalog.Column) (*searchField, error) {
	pgTypeName := column.DataType
	typeName, isArray, err := m.parsePGType(pgTypeName)
	if err != nil {
		return nil, fmt.Errorf("pg to search type: failed to parse pg type: %w", err)
	}

	metadata := metadata{}

	var searchType searchType
	switch typeName {
	case "int8", "int2", "int4", "integer", "smallint", "bigint":
		searchType = searchTypeInteger
	case "float4", "float8", "real", "double precision", "float", "numeric":
		searchType = searchTypeFloat
	case "boolean":
		searchType = searchTypeBool
	case "bytea", "char", "name", "text", "varchar", "bpchar", "xml", "uuid", "character varying", "character", "cidr", "inet", "macaddr", "macaddr8", "interval":
		searchType = searchTypeString
	case "jsonb", "json":
		searchType = searchTypeJSON
	case "date":
		searchType = searchTypeDate
	case "time", "time with time zone", "time without time zone":
		searchType = searchTypeTime
	case "timestamp", "timestamp without time zone":
		searchType = searchTypeDateTime
	case "timestamptz", "timetz", "timestamp with time zone":
		searchType = searchTypeDateTimeTZ
	default:
		// pgvector includes the schema (sometimes? seems only a problem when testing locally)
		if isPGVector(typeName) {
			searchType = searchTypePGVector
			metadata.vectorDimension, err = getPGVectorDimension(typeName)
			if err != nil {
				return nil, search.ErrTypeInvalid{Input: pgTypeName}
			}
		} else {
			return nil, search.ErrTypeInvalid{Input: pgTypeName}
		}
	}

	return &searchField{
		searchType: searchType,
		isArray:    isArray,
		metadata:   metadata,
	}, nil
}

func (m *Mapper) mapDateTimeArray(searchField *searchField, value any) (any, error) {
	switch searchField.searchType {
	case searchTypeDateTimeTZ:
		var a pgtype.FlatArray[pgtype.Timestamptz]
		err := m.pgTypeMap.SQLScanner(&a).Scan(value)
		if err != nil {
			return nil, fmt.Errorf("mapping timestamptz array from pg to ES failed: %w (value: %s)", err, value)
		}

		dts := make([]string, len(a))

		for i := range a {
			dts[i] = a[i].Time.Truncate(time.Millisecond).Format(timestampTZFormat)
		}

		return dts, nil
	case searchTypeDateTime:
		var a pgtype.FlatArray[pgtype.Timestamp]
		err := m.pgTypeMap.SQLScanner(&a).Scan(value)
		if err != nil {
			return nil, fmt.Errorf("mapping timestampt array from pg to ES failed: %w (value: %s)", err, value)
		}

		dts := make([]string, len(a))

		for i := range a {
			dts[i] = a[i].Time.Truncate(time.Millisecond).Format(timestampFormat)
		}

		return dts, nil
	}
	return value, nil
}

func (m *Mapper) mapDateTime(searchField *searchField, value any) (any, error) {
	switch searchField.searchType {
	case searchTypeDateTimeTZ:
		var ts pgtype.Timestamptz
		if err := ts.Scan(value); err != nil {
			return nil, fmt.Errorf("mapping timestamptz from pg to ES failed: %w (value: %s)", err, value)
		}
		return ts.Time.Truncate(time.Millisecond).Format(timestampTZFormat), nil
	case searchTypeDateTime:
		var ts pgtype.Timestamp
		if err := ts.Scan(value); err != nil {
			return nil, fmt.Errorf("mapping timestamp from pg to ES failed: %w (value: %s)", err, value)
		}
		return ts.Time.Truncate(time.Millisecond).Format(timestampFormat), nil
	}
	return value, nil
}

func (m *Mapper) parsePGType(name string) (typeName string, isArray bool, err error) {
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
