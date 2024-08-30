// SPDX-License-Identifier: Apache-2.0

package searchstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type SearchRequest struct {
	Index          *string
	ReturnVersion  *bool
	Size           *int
	From           *int
	Sort           *string
	SourceIncludes *string
	Query          io.Reader
}

type DeleteByQueryRequest struct {
	Index   []string
	Query   map[string]any
	Refresh bool
}

type IndexRequest struct {
	Index   string
	Body    []byte
	Refresh string
}

type IndexWithIDRequest struct {
	Index   string
	ID      string
	Body    []byte
	Refresh string
}

type BulkItem struct {
	Index  *BulkIndex      `json:"index,omitempty"`
	Delete *BulkIndex      `json:"delete,omitempty"`
	Doc    map[string]any  `json:"-"`
	Status int             `json:"-"`
	Error  json.RawMessage `json:"-"`
}

type BulkIndex struct {
	Index       string `json:"_index"`
	ID          string `json:"_id"`
	Version     *int   `json:"version,omitempty"`
	VersionType string `json:"version_type,omitempty"`
}

type BulkResponseItem struct {
	Index struct {
		Status int             `json:"status"`
		Error  json.RawMessage `json:"error"`
	} `json:"index"`
	Delete struct {
		Status int             `json:"status"`
		Error  json.RawMessage `json:"error"`
	} `json:"delete"`
}

type Highlight struct {
	Fields   map[string]any `json:"fields"`
	Encoder  string         `json:"encoder"`
	PreTags  []string       `json:"pre_tags,omitempty"`
	PostTags []string       `json:"post_tags,omitempty"`
}

type BoolFilter struct {
	Filter             []Condition `json:"filter,omitempty"`
	Should             []Condition `json:"should,omitempty"`
	Must               []Condition `json:"must,omitempty"`
	MustNot            []Condition `json:"must_not,omitempty"`
	MinimumShouldMatch int         `json:"minimum_should_match,omitempty"`
}

type ExistsFilter struct {
	Field string `json:"field"`
}

type Condition struct {
	Term       map[string]any `json:"term,omitempty"`
	Terms      map[string]any `json:"terms,omitempty"`
	Prefix     map[string]any `json:"prefix,omitempty"`
	Wildcard   map[string]any `json:"wildcard,omitempty"`
	IDs        map[string]any `json:"ids,omitempty"`
	Range      map[string]any `json:"range,omitempty"`
	Exists     *ExistsFilter  `json:"exists,omitempty"`
	Bool       *BoolFilter    `json:"bool,omitempty"`
	MultiMatch *MultiMatch    `json:"multi_match,omitempty"`
}

type QueryBody struct {
	Query      *Query                  `json:"query,omitempty"`
	Sort       *Sort                   `json:"sort,omitempty"`
	Highlight  *Highlight              `json:"highlight,omitempty"`
	Aggs       *map[string]Aggregation `json:"aggs,omitempty"`
	Size       int                     `json:"size,omitempty"`
	PostFilter *Condition              `json:"post_filter,omitempty"`
}

type Query struct {
	Bool          *BoolFilter          `json:"bool,omitempty"`
	FunctionScore *FunctionScore       `json:"function_score,omitempty"`
	ScriptScore   *ScriptScore         `json:"script_score,omitempty"`
	KNN           *map[string]KNNQuery `json:"knn,omitempty"`
}

type KNNQuery struct {
	Vector []float32 `json:"vector"`
	K      int       `json:"k"`
}

type FunctionScore struct {
	Query     *Query     `json:"query,omitempty"`
	ScoreMode string     `json:"score_mode,omitempty"`
	BoostMode string     `json:"boost_mode,omitempty"`
	Boost     float64    `json:"boost,omitempty"`
	Functions []Function `json:"functions,omitempty"`
}

type Function struct {
	Filter           *Condition             `json:"filter,omitempty"`
	FieldValueFactor *FieldValueFactor      `json:"field_value_factor,omitempty"`
	Weight           *float64               `json:"weight,omitempty"`
	Exp              map[string]ExpFunction `json:"exp,omitempty"`
}

type ExpFunction struct {
	Origin *string `json:"origin,omitempty"`
	Scale  string  `json:"scale"`
	Decay  float64 `json:"decay"`
}

type FieldValueFactor struct {
	Field    string  `json:"field"`
	Factor   float32 `json:"factor"`
	Modifier int     `json:"modifier"`
}

type ScriptScore struct {
	Query  json.RawMessage `json:"query,omitempty"`
	Script *Script         `json:"script,omitempty"`
}

type Script struct {
	Source string         `json:"source"`
	Lang   string         `json:"lang"`
	Params map[string]any `json:"params,omitempty"`
}

type Sort []map[string]any

type MultiMatch struct {
	Query         string   `json:"query"`
	Type          string   `json:"type"`
	Fuzziness     int      `json:"fuzziness,omitempty"`
	Fields        []string `json:"fields,omitempty"`
	Lenient       bool     `json:"lenient,omitempty"`
	MaxExpansions int      `json:"max_expansions,omitempty"`
}

type Aggregation struct {
	Filter        *Condition             `json:"filter,omitempty"`
	Cardinality   *CardinalityAgg        `json:"cardinality,omitempty"`
	DateHistogram *DateHistogramAgg      `json:"date_histogram,omitempty"`
	Sum           *SumMinMaxAgg          `json:"sum,omitempty"`
	Min           *SumMinMaxAgg          `json:"min,omitempty"`
	Max           *SumMinMaxAgg          `json:"max,omitempty"`
	Avg           *SumMinMaxAgg          `json:"avg,omitempty"`
	Percentiles   *ESPercentilesAgg      `json:"percentiles,omitempty"`
	Terms         *TermsAgg              `json:"terms,omitempty"`
	Histogram     *HistogramAgg          `json:"histogram,omitempty"`
	Aggs          map[string]Aggregation `json:"aggs,omitempty"`
}

type CardinalityAgg struct {
	Field              string `json:"field"`
	PrecisionThreshold int    `json:"precision_threshold"`
}

type DateHistogramAgg struct {
	Field            string  `json:"field"`
	FixedInterval    *string `json:"fixed_interval,omitempty"`
	CalendarInterval *string `json:"calendar_interval,omitempty"`
	TimeZone         *string `json:"time_zone,omitempty"`
	Format           string  `json:"format,omitempty"`
}

type SumMinMaxAgg struct {
	Field string `json:"field"`
}

type ESPercentilesAgg struct {
	Field    string    `json:"field"`
	Percents []float32 `json:"percents"`
}

type TermsAgg struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

type HistogramAgg struct {
	Field    string   `json:"field"`
	Interval float64  `json:"interval"`
	Offset   *float64 `json:"offset,omitempty"`
}

type Hit struct {
	ID        string         `json:"_id"`
	Index     string         `json:"_index"`
	Version   int            `json:"_version"`
	Source    map[string]any `json:"_source"`
	Score     float64        `json:"_score"`
	Highlight map[string]any `json:"highlight"`
}

type Hits struct {
	Total struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	Hits []Hit `json:"hits"`
}

type SearchResponse struct {
	Hits         Hits           `json:"hits"`
	Aggregations map[string]any `json:"aggregations"`
}

type BulkResponse struct {
	Errors bool `json:"errors"`
	Items  []BulkResponseItem
}

type IndexStats struct {
	Index            string `json:"index,omitempty"`
	PrimarySizeBytes uint64 `json:"primary_size_bytes,omitempty"`
	TotalSizeBytes   uint64 `json:"total_size_bytes,omitempty"`
}

type Mappings struct {
	Properties map[string]any
	Dynamic    string
}

type MappingResponse map[string]struct {
	Mappings Mappings
}

type IndexStatsResponse struct {
	Indices map[string]struct {
		Primaries struct {
			Store struct {
				SizeInBytes int `json:"size_in_bytes"`
			} `json:"store"`
		} `json:"primaries"`
		Total struct {
			Store struct {
				SizeInBytes int `json:"size_in_bytes"`
			} `json:"store"`
		} `json:"total"`
	} `json:"indices"`
}

type CountResponse struct {
	Count int `json:"count"`
}

func EncodeBulkItems(buffer *bytes.Buffer, items []BulkItem) error {
	encoder := json.NewEncoder(buffer)

	for _, item := range items {
		if err := encoder.Encode(item); err != nil {
			return fmt.Errorf("bulk item [%v]: encode item action %w", item, err)
		}

		if item.Delete != nil {
			continue
		}

		if item.Doc == nil {
			buffer.WriteString("{}\n")
			continue
		}

		if err := encoder.Encode(item.Doc); err != nil {
			return fmt.Errorf("bulk item [%v]: encode item document action %w", item, err)
		}
	}

	return nil
}
