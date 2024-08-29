// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"github.com/xataio/pgstream/internal/searchstore"
)

type Mapper struct{}

const (
	openSearchDefaultEFSearch = 100

	// Lucene’s term byte-length limit is 32766. To cover for the use of UTF-8
	// text with many non-ASCII characters, the maximum value should be 32766 /
	// 4 = 8191 since UTF-8 characters may occupy at most 4 bytes.
	termByteLengthLimit = 32766
)

func NewMapper() *Mapper {
	return &Mapper{}
}

func (m *Mapper) GetDefaultIndexSettings() map[string]any {
	return map[string]any{
		"number_of_shards":                 1,
		"number_of_replicas":               1,
		"index.mapping.total_fields.limit": 2000,
		"index.knn":                        true,
		"knn.algo_param.ef_search":         openSearchDefaultEFSearch,
	}
}

func (m *Mapper) FieldMapping(field *searchstore.Field) (map[string]any, error) {
	switch field.SearchType {
	case searchstore.IntegerType:
		return map[string]any{"type": "long"}, nil
	case searchstore.FloatType:
		return map[string]any{"type": "double"}, nil
	case searchstore.BoolType:
		return map[string]any{"type": "boolean"}, nil
	case searchstore.TextType, searchstore.JSONType:
		return map[string]any{"type": "text"}, nil
	case searchstore.StringType:
		return map[string]any{
			"type":         "keyword",
			"ignore_above": termByteLengthLimit,
			"fields": map[string]any{
				"text": map[string]any{
					"type": "text",
				},
			},
		}, nil
	case searchstore.TimeType:
		return map[string]any{
			"type":   "date",
			"format": "HH:mm:ss[.SS][x][Z]||HH:mm:ss[.SSS][x][Z]||HH:mm:ss[.SSSSSS][x][Z]",
		}, nil
	case searchstore.DateType:
		return map[string]any{
			"type":   "date",
			"format": "date",
		}, nil
	case searchstore.DateTimeType, searchstore.DateTimeTZType:
		return map[string]any{
			"type":   "date",
			"format": "yyyy-MM-dd HH:mm:ss[.SSS][x]||yyyy-MM-dd HH:mm:ss[.SS][x]||yyyy-MM-dd HH:mm:ss[.S][x]||yyyy-MM-dd'T'HH:mm:ss[.SSS][X]",
		}, nil
	case searchstore.PGVectorType:
		vectorSettings := map[string]any{
			"type":      "knn_vector",
			"dimension": field.Metadata.VectorDimension,
		}
		return vectorSettings, nil
	default:
		return nil, searchstore.ErrUnsupportedSearchFieldType
	}
}
