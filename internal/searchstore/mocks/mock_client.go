// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"net/http"

	"github.com/xataio/pgstream/internal/searchstore"
)

type Client struct {
	CloseIndexFn       func(ctx context.Context, index string) error
	CountFn            func(ctx context.Context, index string) (int, error)
	CreateIndexFn      func(ctx context.Context, index string, body map[string]any) error
	DeleteByQueryFn    func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error
	DeleteIndexFn      func(ctx context.Context, index []string) error
	GetIndexAliasFn    func(ctx context.Context, name string) (map[string]any, error)
	GetIndexMappingsFn func(ctx context.Context, index string) (*searchstore.Mappings, error)
	GetIndicesStatsFn  func(ctx context.Context, pattern string) ([]searchstore.IndexStats, error)
	IndexFn            func(ctx context.Context, req *searchstore.IndexRequest) error
	IndexWithIDFn      func(ctx context.Context, req *searchstore.IndexWithIDRequest) error
	IndexExistsFn      func(ctx context.Context, index string) (bool, error)
	ListIndicesFn      func(ctx context.Context, indices []string) ([]string, error)
	PerformFn          func(req *http.Request) (*http.Response, error)
	PutIndexAliasFn    func(ctx context.Context, index []string, name string) error
	PutIndexMappingsFn func(ctx context.Context, index string, body map[string]any) error
	PutIndexSettingsFn func(ctx context.Context, index string, body map[string]any) error
	RefreshIndexFn     func(ctx context.Context, index string) error
	SearchFn           func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error)
	SendBulkRequestFn  func(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error)
	GetMapperFn        func() searchstore.Mapper
}

func (m *Client) CloseIndex(ctx context.Context, index string) error {
	return m.CloseIndexFn(ctx, index)
}

func (m *Client) Count(ctx context.Context, index string) (int, error) {
	return m.CountFn(ctx, index)
}

func (m *Client) CreateIndex(ctx context.Context, index string, body map[string]any) error {
	return m.CreateIndexFn(ctx, index, body)
}

func (m *Client) DeleteByQuery(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
	return m.DeleteByQueryFn(ctx, req)
}

func (m *Client) DeleteIndex(ctx context.Context, index []string) error {
	return m.DeleteIndexFn(ctx, index)
}

func (m *Client) GetIndexAlias(ctx context.Context, name string) (map[string]any, error) {
	return m.GetIndexAliasFn(ctx, name)
}

func (m *Client) GetIndexMappings(ctx context.Context, index string) (*searchstore.Mappings, error) {
	return m.GetIndexMappingsFn(ctx, index)
}

func (m *Client) GetIndicesStats(ctx context.Context, pattern string) ([]searchstore.IndexStats, error) {
	return m.GetIndicesStatsFn(ctx, pattern)
}

func (m *Client) Index(ctx context.Context, req *searchstore.IndexRequest) error {
	return m.IndexFn(ctx, req)
}

func (m *Client) IndexWithID(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
	return m.IndexWithIDFn(ctx, req)
}

func (m *Client) IndexExists(ctx context.Context, index string) (bool, error) {
	return m.IndexExistsFn(ctx, index)
}

func (m *Client) ListIndices(ctx context.Context, indices []string) ([]string, error) {
	return m.ListIndicesFn(ctx, indices)
}

func (m *Client) Perform(req *http.Request) (*http.Response, error) {
	return m.PerformFn(req)
}

func (m *Client) PutIndexAlias(ctx context.Context, index []string, name string) error {
	return m.PutIndexAliasFn(ctx, index, name)
}

func (m *Client) PutIndexMappings(ctx context.Context, index string, body map[string]any) error {
	return m.PutIndexMappingsFn(ctx, index, body)
}

func (m *Client) PutIndexSettings(ctx context.Context, index string, body map[string]any) error {
	return m.PutIndexSettingsFn(ctx, index, body)
}

func (m *Client) RefreshIndex(ctx context.Context, index string) error {
	return m.RefreshIndexFn(ctx, index)
}

func (m *Client) Search(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
	return m.SearchFn(ctx, req)
}

func (m *Client) SendBulkRequest(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error) {
	return m.SendBulkRequestFn(ctx, items)
}

func (m *Client) GetMapper() searchstore.Mapper {
	return m.GetMapperFn()
}
