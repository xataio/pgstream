// SPDX-License-Identifier: Apache-2.0

package searchstore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/xataio/pgstream/internal/json"
)

type Client interface {
	CloseIndex(ctx context.Context, index string) error
	Count(ctx context.Context, index string) (int, error)
	CreateIndex(ctx context.Context, index string, body map[string]any) error
	DeleteByQuery(ctx context.Context, req *DeleteByQueryRequest) error
	DeleteIndex(ctx context.Context, index []string) error
	GetIndexAlias(ctx context.Context, name string) (map[string]any, error)
	GetIndexMappings(ctx context.Context, index string) (*Mappings, error)
	GetIndicesStats(ctx context.Context, indexPattern string) ([]IndexStats, error)
	Index(ctx context.Context, req *IndexRequest) error
	IndexWithID(ctx context.Context, req *IndexWithIDRequest) error
	IndexExists(ctx context.Context, index string) (bool, error)
	ListIndices(ctx context.Context, indices []string) ([]string, error)
	Perform(req *http.Request) (*http.Response, error)
	PutIndexAlias(ctx context.Context, index []string, name string) error
	PutIndexMappings(ctx context.Context, index string, body map[string]any) error
	PutIndexSettings(ctx context.Context, index string, body map[string]any) error
	RefreshIndex(ctx context.Context, index string) error
	Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error)
	SendBulkRequest(ctx context.Context, items []BulkItem) ([]BulkItem, error)
	GetMapper() Mapper
}

func Ptr[T any](i T) *T { return &i }

// createReader returns a reader on the JSON representation of the given value.
func CreateReader(value any) (*bytes.Reader, error) {
	bytesValue, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("unexpected marshaling error: %w", err)
	}
	return bytes.NewReader(bytesValue), nil
}

func VerifyResponse(bodyBytes []byte, items []BulkItem) (failed []BulkItem, err error) {
	var response BulkResponse

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return nil, fmt.Errorf("error unmarshaling response from search store: %w (%s)", err, bodyBytes)
	}

	if !response.Errors {
		return []BulkItem{}, nil
	}

	failed = []BulkItem{}
	for i, respItem := range response.Items {
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
