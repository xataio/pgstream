// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/searchstore"
	searchstoremocks "github.com/xataio/pgstream/internal/searchstore/mocks"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	searchmocks "github.com/xataio/pgstream/pkg/wal/processor/search/mocks"
)

func TestStore_ApplySchemaChange(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	id := xid.New()
	testLogEntry := &schemalog.LogEntry{
		ID:         id,
		SchemaName: testSchemaName,
		Version:    0,
	}
	newLogEntry := &schemalog.LogEntry{
		ID:         id,
		SchemaName: testSchemaName,
		Version:    1,
	}

	testSearchResponse := &searchstore.SearchResponse{
		Hits: searchstore.Hits{
			Hits: []searchstore.Hit{
				{ID: "doc-1"},
			},
		},
	}

	testAdapter := &mockAdapter{
		recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
			return testLogEntry, nil
		},
		schemaNameToIndexFn: newDefaultIndexName,
	}

	testMapper := &searchmocks.Mapper{
		ColumnToSearchMappingFn: func(column schemalog.Column) (map[string]any, error) {
			return map[string]any{
				"test": "mapping",
			}, nil
		},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name     string
		client   searchstore.Client
		logEntry *schemalog.LogEntry

		wantErr error
	}{
		{
			name: "ok - nil entry",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
			},
			logEntry: nil,

			wantErr: nil,
		},
		{
			name: "ok",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return testSearchResponse, nil
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) { return true, nil },
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
			},
			logEntry: newLogEntry,

			wantErr: nil,
		},
		{
			name: "ok - index doesn't exist",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, searchstore.ErrResourceNotFound
				},
				IndexExistsFn:   func(ctx context.Context, index string) (bool, error) { return false, nil },
				CreateIndexFn:   func(ctx context.Context, index string, body map[string]any) error { return nil },
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error { return nil },
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
			},
			logEntry: newLogEntry,

			wantErr: nil,
		},
		{
			name: "error - ensuring schema exists",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, searchstore.ErrResourceNotFound
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					if index == schemalogIndexName {
						return true, nil
					}
					return false, nil
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error { return errTest },
			},
			logEntry: newLogEntry,

			wantErr: errTest,
		},
		{
			name: "error - getting last schema",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, errTest
				},
			},
			logEntry: newLogEntry,

			wantErr: errTest,
		},
		{
			name: "error - schema out of order",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return testSearchResponse, nil
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) { return true, nil },
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
			},
			logEntry: testLogEntry,

			wantErr: search.ErrSchemaUpdateOutOfOrder{
				SchemaName:     testSchemaName,
				SchemaID:       id.String(),
				NewVersion:     0,
				CurrentVersion: 0,
			},
		},
		{
			name: "error - updating mapping",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return testSearchResponse, nil
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) { return true, nil },
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error { return errTest },
			},
			logEntry: newLogEntry,

			wantErr: errTest,
		},
		{
			name: "error - ensuring schema mapping",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return testSearchResponse, nil
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) { return false, nil },
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error { return errTest },
			},
			logEntry: newLogEntry,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			s.adapter = testAdapter
			s.mapper = testMapper

			err := s.ApplySchemaChange(context.Background(), tc.logEntry)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_SendDocuments(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	testDocs := []search.Document{
		{
			ID:     "1",
			Schema: testSchemaName,
		},
	}
	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		client searchstore.Client

		wantErrDocs []search.DocumentError
		wantErr     error
	}{
		{
			name: "ok - no failed documents",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SendBulkRequestFn: func(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error) {
					return nil, nil
				},
			},

			wantErrDocs: nil,
			wantErr:     nil,
		},
		{
			name: "ok - with failed documents",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SendBulkRequestFn: func(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error) {
					return []searchstore.BulkItem{
						{
							Index: &searchstore.BulkIndex{
								Index: testSchemaName,
								ID:    "doc-1",
							},
							Status: http.StatusBadRequest,
							Error:  []byte("oh noes"),
						},
					}, nil
				},
			},

			wantErrDocs: []search.DocumentError{
				{
					Document: search.Document{
						ID:     "doc-1",
						Schema: testSchemaName,
					},
					Severity: search.SeverityDataLoss,
					Error:    "oh noes",
				},
			},
			wantErr: nil,
		},
		{
			name: "error - sending bulk request",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SendBulkRequestFn: func(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error) {
					return nil, errTest
				},
			},

			wantErrDocs: nil,
			wantErr:     errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)

			errDocs, err := s.SendDocuments(context.Background(), testDocs)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantErrDocs, errDocs)
		})
	}
}

func TestStore_DeleteSchema(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	testIndexWithVersion := "test_schema-1"
	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		client searchstore.Client

		wantErr error
	}{
		{
			name: "ok",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, testIndexWithVersion, index)
					return true, nil
				},
				DeleteIndexFn: func(ctx context.Context, index []string) error {
					require.Equal(t, []string{testIndexWithVersion}, index)
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					require.Equal(t, []string{schemalogIndexName}, req.Index)
					require.Equal(t, map[string]any{
						"query": map[string]any{
							"term": map[string]any{
								"schema_name": testSchemaName,
							},
						},
					}, req.Query)
					require.Equal(t, true, req.Refresh)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - index doesn't exist",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, testIndexWithVersion, index)
					return false, nil
				},
				DeleteIndexFn: func(ctx context.Context, index []string) error {
					return errors.New("DeleteIndexFn: should not be called")
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					require.Equal(t, []string{schemalogIndexName}, req.Index)
					require.Equal(t, map[string]any{
						"query": map[string]any{
							"term": map[string]any{
								"schema_name": testSchemaName,
							},
						},
					}, req.Query)
					require.Equal(t, true, req.Refresh)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - checking index exists",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return false, errTest
				},
				DeleteIndexFn: func(ctx context.Context, index []string) error {
					return errors.New("DeleteIndexFn: should not be called")
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errors.New("DeleteByQueryFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - deleting index",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return true, nil
				},
				DeleteIndexFn: func(ctx context.Context, index []string) error {
					return errTest
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errors.New("DeleteByQueryFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - deleting schema from schema log",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return false, nil
				},
				DeleteIndexFn: func(ctx context.Context, index []string) error {
					return errTest
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			err := s.DeleteSchema(context.Background(), testSchemaName)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_DeleteTableDocuments(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	testTableIDs := []string{"table-1", "table-2"}
	errTest := errors.New("oh noes")

	tests := []struct {
		name     string
		client   searchstore.Client
		tableIDs []string

		wantErr error
	}{
		{
			name: "ok",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					require.Equal(t, []string{testSchemaName}, req.Index)
					require.Equal(t, map[string]any{
						"query": map[string]any{
							"terms": map[string]any{
								"_table": testTableIDs,
							},
						},
					}, req.Query)
					require.Equal(t, true, req.Refresh)
					return nil
				},
			},
			tableIDs: testTableIDs,

			wantErr: nil,
		},
		{
			name: "ok - no tables",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					require.Equal(t, []string{testSchemaName}, req.Index)
					require.Equal(t, map[string]any{
						"query": map[string]any{
							"terms": map[string]any{
								"_table": testTableIDs,
							},
						},
					}, req.Query)
					require.Equal(t, true, req.Refresh)
					return nil
				},
			},
			tableIDs: nil,

			wantErr: nil,
		},
		{
			name: "error - deleting by query",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errTest
				},
			},
			tableIDs: testTableIDs,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			err := s.DeleteTableDocuments(context.Background(), testSchemaName, tc.tableIDs)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_getLastSchemaLogEntry(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	testLogEntry := &schemalog.LogEntry{
		ID:         xid.New(),
		SchemaName: testSchemaName,
	}
	testBody := []byte("test-body")

	errTest := errors.New("oh noes")

	tests := []struct {
		name      string
		client    searchstore.Client
		adapter   SearchAdapter
		marshaler func(any) ([]byte, error)

		wantLogEntry *schemalog.LogEntry
		wantErr      error
	}{
		{
			name: "ok",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					require.Equal(t, &searchstore.SearchRequest{
						Index: searchstore.Ptr(schemalogIndexName),
						Size:  searchstore.Ptr(1),
						Sort:  searchstore.Ptr("version:desc"),
						Query: bytes.NewBuffer(testBody),
					}, req)
					return &searchstore.SearchResponse{
						Hits: searchstore.Hits{
							Hits: []searchstore.Hit{
								{ID: "doc-1"},
							},
						},
					}, nil
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return testLogEntry, nil
				},
			},
			marshaler: func(a any) ([]byte, error) { return testBody, nil },

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "error - marshaling search query",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, errors.New("SearchFn: should not be called")
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return nil, errors.New("recordToLogEntryFn: should not be called")
				},
			},
			marshaler: func(a any) ([]byte, error) { return nil, errTest },

			wantLogEntry: nil,
			wantErr:      errTest,
		},
		{
			name: "error - no hits in response",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return &searchstore.SearchResponse{
						Hits: searchstore.Hits{},
					}, nil
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return nil, errors.New("recordToLogEntryFn: should not be called")
				},
			},

			wantLogEntry: nil,
			wantErr:      search.ErrSchemaNotFound{SchemaName: testSchemaName},
		},
		{
			name: "error - schema not found",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, searchstore.ErrResourceNotFound
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, schemalogIndexName, index)
					return true, nil
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return nil, errors.New("recordToLogEntryFn: should not be called")
				},
			},

			wantLogEntry: nil,
			wantErr:      search.ErrSchemaNotFound{SchemaName: testSchemaName},
		},
		{
			name: "error - retrieving schema",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, errTest
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return nil, errors.New("recordToLogEntryFn: should not be called")
				},
			},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
		{
			name: "error - schema not found with pgstream index creation",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, searchstore.ErrResourceNotFound
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, schemalogIndexName, index)
					return false, nil
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, schemalogIndexName, index)
					return nil
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return nil, errors.New("recordToLogEntryFn: should not be called")
				},
			},

			wantLogEntry: nil,
			wantErr:      search.ErrSchemaNotFound{SchemaName: testSchemaName},
		},
		{
			name: "error - schema not found, failed to create schemalog index",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				SearchFn: func(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
					return nil, searchstore.ErrResourceNotFound
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, schemalogIndexName, index)
					return false, nil
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					return errTest
				},
			},
			adapter: &mockAdapter{
				recordToLogEntryFn: func(m map[string]any) (*schemalog.LogEntry, error) {
					return nil, errors.New("recordToLogEntryFn: should not be called")
				},
			},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			if tc.adapter != nil {
				s.adapter = tc.adapter
			}
			if tc.marshaler != nil {
				s.marshaler = tc.marshaler
			}

			logEntry, err := s.getLastSchemaLogEntry(context.Background(), testSchemaName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLogEntry, logEntry)
		})
	}
}

func TestStore_createSchema(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		client searchstore.Client

		wantErr error
	}{
		{
			name: "ok",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					return nil
				},
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error {
					require.Equal(t, []string{fmt.Sprintf("%s-1", testSchemaName)}, index)
					require.Equal(t, testSchemaName, name)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - creating index",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					return errTest
				},
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error {
					return errors.New("PutIndexAliasFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - putting index alias",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					return nil
				},
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)

			err := s.createSchema(context.Background(), testSchemaName)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_updateMapping(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	testIndexName := testSchemaName
	testLogEntry := &schemalog.LogEntry{
		ID:         xid.New(),
		SchemaName: testSchemaName,
	}

	testMapping := map[string]any{
		"test": "mapping",
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		client searchstore.Client
		diff   *schemalog.SchemaDiff
		mapper search.Mapper

		wantErr error
	}{
		{
			name: "ok - no diff",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errors.New("PutIndexMappingsFn: should not be called")
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errors.New("DeleteByQueryFn: should not be called")
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - diff with columns to add",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testIndexName, index)
					require.Equal(t, map[string]any{
						"properties": map[string]any{
							"pgstreamid-1": testMapping,
						},
					}, body)
					return nil
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errors.New("DeleteByQueryFn: should not be called")
				},
			},
			diff: &schemalog.SchemaDiff{
				ColumnsToAdd: []schemalog.Column{
					{Name: "col-1", PgstreamID: "pgstreamid-1"},
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column schemalog.Column) (map[string]any, error) {
					return testMapping, nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - diff with tables to remove",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errors.New("PutIndexMappingsFn: should not be called")
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					require.Equal(t, []string{testIndexName}, req.Index)
					require.Equal(t, map[string]any{
						"query": map[string]any{
							"terms": map[string]any{
								"_table": []string{"id-1", "id-2"},
							},
						},
					}, req.Query)
					require.Equal(t, true, req.Refresh)
					return nil
				},
			},
			diff: &schemalog.SchemaDiff{
				TablesToRemove: []schemalog.Table{
					{PgstreamID: "id-1"},
					{PgstreamID: "id-2"},
				},
			},

			wantErr: nil,
		},
		{
			name: "error - updating mapping",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errTest
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return errors.New("IndexWithIDFn: should not be called")
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errors.New("DeleteByQueryFn: should not be called")
				},
			},
			diff: &schemalog.SchemaDiff{
				ColumnsToAdd: []schemalog.Column{
					{Name: "col-1", PgstreamID: "pgstreamid-1"},
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - deleting tables",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errors.New("PutIndexMappingsFn: should not be called")
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return errors.New("IndexWithIDFn: should not be called")
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errTest
				},
			},
			diff: &schemalog.SchemaDiff{
				TablesToRemove: []schemalog.Table{
					{PgstreamID: "id-1"},
					{PgstreamID: "id-2"},
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - inserting schemalog",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errors.New("PutIndexMappingsFn: should not be called")
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return errTest
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return errors.New("DeleteByQueryFn: should not be called")
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			if tc.mapper != nil {
				s.mapper = tc.mapper
			}

			err := s.updateMapping(context.Background(), testSchemaName, testLogEntry, tc.diff)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
