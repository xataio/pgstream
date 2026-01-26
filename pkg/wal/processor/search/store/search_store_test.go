// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/searchstore"
	searchstoremocks "github.com/xataio/pgstream/internal/searchstore/mocks"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	searchmocks "github.com/xataio/pgstream/pkg/wal/processor/search/mocks"
)

func TestStore_ApplySchemaDiff(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	testIndexName := fmt.Sprintf("%s-1", testSchemaName)
	errTest := errors.New("oh noes")

	testMapping := map[string]any{
		"type": "text",
	}

	tests := []struct {
		name   string
		client searchstore.Client
		mapper search.Mapper
		diff   *wal.SchemaDiff

		wantErr error
	}{
		{
			name: "ok - empty diff",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
			},
			diff: &wal.SchemaDiff{},

			wantErr: nil,
		},
		{
			name: "ok - diff with new columns",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, testIndexName, index)
					return true, nil
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testSchemaName, index)
					return nil
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
					return testMapping, nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesAdded: []wal.DDLObject{
					{
						Identity:   "public.table1",
						PgstreamID: "pgstreamid",
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "col1", Type: "text"},
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - diff with tables removed",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return true, nil
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					require.Equal(t, []string{testSchemaName}, req.Index)
					return nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesRemoved: []wal.DDLObject{
					{Identity: "public.table1", PgstreamID: "table1_id"},
					{Identity: "public.table2", PgstreamID: "table2_id"},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - diff with table primary key change",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return true, nil
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesChanged: []wal.TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstreamid",
						TablePrimaryKeyChange: &wal.ValueChange[[]string]{
							Old: []string{"id"},
							New: []string{"uuid"},
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - create schema when it doesn't exist",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, testIndexName, index)
					return false, nil
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testIndexName, index)
					return nil
				},
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error {
					require.Equal(t, []string{testIndexName}, index)
					require.Equal(t, testSchemaName, name)
					return nil
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return nil
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
					return testMapping, nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesAdded: []wal.DDLObject{
					{
						Identity:   "public.table1",
						PgstreamID: "pgstreamid",
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "col1", Type: "text"},
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - create schema that doesn't exist but it does when trying to create",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, testIndexName, index)
					return false, nil
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					return search.ErrSchemaAlreadyExists{}
				},
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error {
					return errors.New("PutIndexAliasFn: should not be called")
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return nil
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
					return testMapping, nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesAdded: []wal.DDLObject{
					{
						Identity:   "public.table1",
						PgstreamID: "pgstreamid",
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "col1", Type: "text"},
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "error - ensuring schema existence",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return false, errTest
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesAdded: []wal.DDLObject{
					{Identity: "public.table1", PgstreamID: "pgstreamid"},
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - updating mapping",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					return true, nil
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errTest
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
					return testMapping, nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesAdded: []wal.DDLObject{
					{
						Identity:   "public.table1",
						PgstreamID: "pgstreamid",
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "col1", Type: "text"},
						},
					},
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - creating schema when it doesn't exist",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				IndexExistsFn: func(ctx context.Context, index string) (bool, error) {
					require.Equal(t, testIndexName, index)
					return false, nil
				},
				CreateIndexFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testIndexName, index)
					return errTest
				},
				PutIndexAliasFn: func(ctx context.Context, index []string, name string) error {
					return errors.New("PutIndexAliasFn: should not be called")
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					return errors.New("PutIndexMappingsFn: should not be called")
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
					return testMapping, nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesAdded: []wal.DDLObject{
					{
						Identity:   "public.table1",
						PgstreamID: "pgstreamid",
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "col1", Type: "text"},
						},
					},
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			if tc.mapper != nil {
				s.mapper = tc.mapper
			}

			err := s.ApplySchemaDiff(context.Background(), tc.diff)
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
	}

	for _, tc := range tests {
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			err := s.DeleteTableDocuments(context.Background(), testSchemaName, tc.tableIDs)
			require.ErrorIs(t, err, tc.wantErr)
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

	testMapping := map[string]any{
		"test": "mapping",
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		client searchstore.Client
		diff   *wal.SchemaDiff
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
			name: "ok - diff with table to add",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testIndexName, index)
					require.Equal(t, map[string]any{
						"properties": map[string]any{
							"pgstreamid-1": testMapping,
							"users.col-1": map[string]any{
								"type": "alias",
								"path": "pgstreamid-1",
							},
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
			diff: &wal.SchemaDiff{
				TablesAdded: []wal.DDLObject{
					{
						Schema:     "public",
						Identity:   "public.users",
						PgstreamID: "pgstreamid",
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "col-1"},
						},
					},
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
					return testMapping, nil
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
							"users.col-1": map[string]any{
								"type": "alias",
								"path": "pgstreamid-1",
							},
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
			diff: &wal.SchemaDiff{
				TablesChanged: []wal.TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstreamid",
						ColumnsAdded: []wal.DDLColumn{
							{Attnum: 1, Name: "col-1"},
						},
					},
				},
			},
			mapper: &searchmocks.Mapper{
				ColumnToSearchMappingFn: func(column *wal.DDLColumn) (map[string]any, error) {
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
			diff: &wal.SchemaDiff{
				TablesRemoved: []wal.DDLObject{
					{PgstreamID: "id-1"},
					{PgstreamID: "id-2"},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - diff with column rename",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testIndexName, index)
					require.Equal(t, map[string]any{
						"properties": map[string]any{
							"users.new_name": map[string]any{
								"type": "alias",
								"path": "pgstreamid-1",
							},
						},
					}, body)
					return nil
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesChanged: []wal.TableDiff{
					{
						TableName:       "users",
						TablePgstreamID: "pgstreamid",
						ColumnsChanged: []wal.ColumnDiff{
							{
								ColumnPgstreamID: "pgstreamid-1",
								NameChange: &wal.ValueChange[string]{
									Old: "old_name",
									New: "new_name",
								},
							},
						},
					},
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - diff with table rename",
			client: &searchstoremocks.Client{
				GetMapperFn: func() searchstore.Mapper {
					return &searchstoremocks.Mapper{}
				},
				PutIndexMappingsFn: func(ctx context.Context, index string, body map[string]any) error {
					require.Equal(t, testIndexName, index)
					require.Equal(t, map[string]any{
						"properties": map[string]any{
							"new_name": map[string]any{
								"col-1": map[string]any{
									"type": "alias",
									"path": "pgstreamid-1",
								},
							},
						},
					}, body)
					return nil
				},
				IndexWithIDFn: func(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
					return nil
				},
				DeleteByQueryFn: func(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
					return nil
				},
				GetIndexMappingsFn: func(ctx context.Context, index string) (*searchstore.Mappings, error) {
					return &searchstore.Mappings{
						Properties: map[string]any{
							"old_name": map[string]any{
								"col-1": map[string]any{
									"type": "alias",
									"path": "pgstreamid-1",
								},
							},
						},
					}, nil
				},
			},
			diff: &wal.SchemaDiff{
				SchemaName: testSchemaName,
				TablesChanged: []wal.TableDiff{
					{
						TableName:       "new_name",
						TablePgstreamID: "pgstreamid",
						TableNameChange: &wal.ValueChange[string]{
							Old: "old_name",
							New: "new_name",
						},
					},
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
			diff: &wal.SchemaDiff{
				TablesChanged: []wal.TableDiff{
					{
						TablePgstreamID: "pgstreamid",
						ColumnsAdded: []wal.DDLColumn{
							{Attnum: 1, Name: "col-1"},
						},
					},
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
			diff: &wal.SchemaDiff{
				TablesRemoved: []wal.DDLObject{
					{PgstreamID: "id-1"},
					{PgstreamID: "id-2"},
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreWithClient(tc.client)
			if tc.mapper != nil {
				s.mapper = tc.mapper
			}

			err := s.updateMapping(context.Background(), testSchemaName, tc.diff)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
