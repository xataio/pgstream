// SPDX-License-Identifier: Apache-2.0

package es

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

type mappingResponse map[string]struct {
	Mappings Mappings
}

type indexStatsResponse struct {
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

type countResponse struct {
	Count int `json:"count"`
}

func encodeBulkItems(buffer *bytes.Buffer, items []BulkItem) error {
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

func verifyResponse(bodyBytes []byte, items []BulkItem) (failed []BulkItem, err error) {
	var esResponse BulkResponse

	if err := json.Unmarshal(bodyBytes, &esResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling response from es: %w (%s)", err, bodyBytes)
	}

	if !esResponse.Errors {
		return []BulkItem{}, nil
	}

	failed = []BulkItem{}
	for i, respItem := range esResponse.Items {
		if items[i].Index != nil {
			if respItem.Index.Status > 299 {
				items[i].Status = respItem.Index.Status
				items[i].Error = respItem.Index.Error
				failed = append(failed, items[i])
			}
		} else if items[i].Delete != nil {
			if respItem.Delete.Status > 299 {
				items[i].Status = respItem.Delete.Status
				items[i].Error = respItem.Delete.Error
				failed = append(failed, items[i])
			}
		}
	}

	return failed, nil
}
