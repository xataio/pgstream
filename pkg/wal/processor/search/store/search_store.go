// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/internal/searchstore"
	elasticsearchstore "github.com/xataio/pgstream/internal/searchstore/elasticsearch"
	opensearchstore "github.com/xataio/pgstream/internal/searchstore/opensearch"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type Store struct {
	logger               loglib.Logger
	client               searchstore.Client
	mapper               search.Mapper
	adapter              SearchAdapter
	indexNameAdapter     IndexNameAdapter
	marshaler            func(any) ([]byte, error)
	defaultIndexSettings map[string]any
}

type Config struct {
	OpenSearchURL    string
	ElasticsearchURL string
}

type Option func(*Store)

const (
	// OpenSearch/Elasticsearch have a limit of 512 bytes for the ID field. see here:
	// https://www.elastic.co/guide/en/elasticsearch/reference/7.10/mapping-id-field.html
	idFieldLengthLimit = 512

	schemalogIndexName = "pgstream"
)

func NewStore(cfg Config, opts ...Option) (*Store, error) {
	var searchStore searchstore.Client
	var err error
	switch {
	case cfg.OpenSearchURL != "" && cfg.ElasticsearchURL != "":
		return nil, errors.New("only one store URL must be provided")
	case cfg.OpenSearchURL == "" && cfg.ElasticsearchURL == "":
		return nil, errors.New("a store URL must be provided")
	case cfg.OpenSearchURL != "":
		searchStore, err = opensearchstore.NewClient(cfg.OpenSearchURL)
	case cfg.ElasticsearchURL != "":
		searchStore, err = elasticsearchstore.NewClient(cfg.ElasticsearchURL)
	}
	if err != nil {
		return nil, fmt.Errorf("create search store client: %w", err)
	}

	s := NewStoreWithClient(searchStore)

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func NewStoreWithClient(client searchstore.Client) *Store {
	mapper := client.GetMapper()
	indexNameAdapter := newDefaultIndexNameAdapter()
	return &Store{
		logger:               loglib.NewNoopLogger(),
		client:               client,
		indexNameAdapter:     indexNameAdapter,
		adapter:              newDefaultAdapter(indexNameAdapter),
		mapper:               NewPostgresMapper(mapper),
		marshaler:            json.Marshal,
		defaultIndexSettings: mapper.GetDefaultIndexSettings(),
	}
}

func WithLogger(l loglib.Logger) Option {
	return func(s *Store) {
		s.logger = loglib.NewLogger(l)
	}
}

func WithMapper(m search.Mapper) Option {
	return func(s *Store) {
		s.mapper = m
	}
}

func WithIndexNameAdapter(a IndexNameAdapter) Option {
	return func(s *Store) {
		s.indexNameAdapter = a
		s.adapter = newDefaultAdapter(a)
	}
}

func (s *Store) GetMapper() search.Mapper {
	return s.mapper
}

func (s *Store) ApplySchemaDiff(ctx context.Context, diff *wal.SchemaDiff) error {
	if diff.IsEmpty() {
		return nil
	}

	if err := s.ensureSchema(ctx, diff.SchemaName); err != nil {
		return fmt.Errorf("ensuring schema existence: %w", err)
	}

	for _, tbl := range diff.TablesChanged {
		if tbl.TablePrimaryKeyChange != nil {
			s.logger.Warn(nil, fmt.Sprintf("primary key identity column changed for table %s, reindexing required", tbl.TableName))
		}
	}

	if err := s.updateMapping(ctx, diff.SchemaName, diff); err != nil {
		return fmt.Errorf("update mapping for schema: %w", err)
	}
	return nil
}

func (s *Store) SendDocuments(ctx context.Context, docs []search.Document) ([]search.DocumentError, error) {
	items := make([]searchstore.BulkItem, 0, len(docs))
	for _, doc := range docs {
		if len(doc.ID) > idFieldLengthLimit {
			s.logger.Error(errors.New("ID is longer than 512 bytes"), "error processing document, skipping", loglib.Fields{
				"severity": "DATALOSS",
				"id":       doc.ID,
			})
			continue
		}
		items = append(items, s.adapter.SearchDocToBulkItem(doc))
	}
	failed, err := s.client.SendBulkRequest(ctx, items)
	if err != nil {
		return nil, mapError(err)
	}

	return s.adapter.BulkItemsToSearchDocErrs(failed), nil
}

func (s *Store) DeleteSchema(ctx context.Context, schemaName string) error {
	index := s.indexNameAdapter.SchemaNameToIndex(schemaName)
	exists, err := s.client.IndexExists(ctx, index.NameWithVersion())
	if err != nil {
		return mapError(err)
	}
	if !exists {
		return nil
	}

	if err := s.client.DeleteIndex(ctx, []string{index.NameWithVersion()}); err != nil {
		return mapError(err)
	}

	// delete the schema from the schema log index
	if err := s.client.DeleteByQuery(ctx, &searchstore.DeleteByQueryRequest{
		Index: []string{schemalogIndexName},
		Query: map[string]any{
			"query": map[string]any{
				"term": map[string]any{
					"schema_name": index.SchemaName(),
				},
			},
		},
		Refresh: true,
	}); err != nil {
		return mapError(err)
	}

	return nil
}

func (s *Store) DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) error {
	index := s.indexNameAdapter.SchemaNameToIndex(schemaName)
	if err := s.deleteTableDocuments(ctx, index, tableIDs); err != nil {
		return mapError(err)
	}
	return nil
}

func (s *Store) schemaExists(ctx context.Context, schemaName string) (bool, error) {
	indexName := s.indexNameAdapter.SchemaNameToIndex(schemaName)
	exists, err := s.client.IndexExists(ctx, indexName.NameWithVersion())
	if err != nil {
		return false, mapError(err)
	}
	return exists, nil
}

func (s *Store) createSchema(ctx context.Context, schemaName string) error {
	index := s.indexNameAdapter.SchemaNameToIndex(schemaName)
	err := s.client.CreateIndex(ctx, index.NameWithVersion(), map[string]any{
		"mappings": map[string]any{
			"dynamic": "strict",
			"properties": map[string]any{
				"_table": map[string]any{
					"type": "keyword",
				},
			},
		},
		"settings": s.defaultIndexSettings,
	})
	if err != nil {
		if errors.As(err, &searchstore.ErrResourceAlreadyExists{}) {
			return &search.ErrSchemaAlreadyExists{
				SchemaName: schemaName,
			}
		}
		return mapError(err)
	}

	if err := s.client.PutIndexAlias(ctx, []string{index.NameWithVersion()}, index.Name()); err != nil {
		return mapError(err)
	}
	return nil
}

func (s *Store) updateMapping(ctx context.Context, schemaName string, diff *wal.SchemaDiff) error {
	index := s.indexNameAdapter.SchemaNameToIndex(schemaName)
	if diff != nil && !diff.IsEmpty() {
		if err := s.updateMappingColumns(ctx, index, diff); err != nil {
			return fmt.Errorf("failed to add new columns: %w", mapError(err))
		}

		if len(diff.TablesRemoved) > 0 {
			tableIDs := make([]string, 0, len(diff.TablesRemoved))
			for _, table := range diff.TablesRemoved {
				tableIDs = append(tableIDs, table.PgstreamID)
			}
			if err := s.deleteTableDocuments(ctx, index, tableIDs); err != nil {
				return fmt.Errorf("failed to delete table documents: %w", mapError(err))
			}
		}
	}

	return nil
}

func (s *Store) deleteTableDocuments(ctx context.Context, index IndexName, tableIDs []string) error {
	if len(tableIDs) == 0 {
		return nil
	}

	req := &searchstore.DeleteByQueryRequest{
		Index: []string{index.Name()},
		Query: map[string]any{
			"query": map[string]any{
				"terms": map[string]any{
					"_table": tableIDs,
				},
			},
		},
		Refresh: true,
	}

	return s.client.DeleteByQuery(ctx, req)
}

func (s *Store) updateMappingColumns(ctx context.Context, indexName IndexName, diff *wal.SchemaDiff) error {
	if diff.IsEmpty() || (len(diff.TablesAdded) == 0 && len(diff.TablesChanged) == 0) {
		return nil
	}

	properties := map[string]any{}

	for _, tbl := range diff.TablesAdded {
		for _, c := range tbl.Columns {
			mapping, err := s.getColumnMapping(indexName.SchemaName(), tbl.GetTable(), tbl.PgstreamID, &c)
			if err != nil {
				return fmt.Errorf("failed to convert column to search mapping: %w", err)
			}
			for k, v := range mapping {
				properties[k] = v
			}
		}
	}

	for _, tbl := range diff.TablesChanged {
		for _, c := range tbl.ColumnsAdded {
			mapping, err := s.getColumnMapping(indexName.SchemaName(), tbl.TableName, tbl.TablePgstreamID, &c)
			if err != nil {
				return fmt.Errorf("failed to convert column to search mapping: %w", err)
			}
			for k, v := range mapping {
				properties[k] = v
			}
		}

		for _, colDiff := range tbl.ColumnsChanged {
			if colDiff.NameChange != nil {
				// add an alias for the new column name
				properties[tbl.TableName+"."+colDiff.NameChange.New] = map[string]any{
					"type": "alias",
					"path": colDiff.ColumnPgstreamID,
				}
			}
		}
		if tbl.TableNameChange != nil {
			// add the old aliases for the new table name
			currentMapping := s.getTableAliasMapping(ctx, indexName, tbl.TableNameChange.Old)
			properties[tbl.TableNameChange.New] = currentMapping
		}
	}

	return s.client.PutIndexMappings(ctx, indexName.Name(), map[string]any{
		"properties": properties,
	})
}

func (s *Store) ensureSchema(ctx context.Context, schemaName string) error {
	exists, err := s.schemaExists(ctx, schemaName)
	if err != nil {
		return fmt.Errorf("checking existence of schema: %w", err)
	}
	if !exists {
		if err := s.createSchema(ctx, schemaName); err != nil {
			if errors.As(err, &search.ErrSchemaAlreadyExists{}) {
				return nil
			}
			return fmt.Errorf("creating schema: %w", err)
		}
	}
	return nil
}

// getTableAliasMapping extract the existing alias mappings for the table on
// input from the provided mapping.
func (s *Store) getTableAliasMapping(ctx context.Context, indexName IndexName, tableName string) any {
	mappings, err := s.client.GetIndexMappings(ctx, indexName.Name())
	if err != nil {
		return fmt.Errorf("failed to get current index mappings: %w", err)
	}
	// Extract the old table name's property structure
	tableProperties, ok := mappings.Properties[tableName]
	if !ok {
		s.logger.Warn(nil, "table name not found in mappings", loglib.Fields{
			"table_name": tableName,
			"index":      indexName.Name(),
			"mappings":   mappings.Properties,
		})
		return nil
	}

	return tableProperties
}

func (s *Store) getColumnMapping(schemaName, tblName, tblPgstreamID string, c *wal.DDLColumn) (map[string]any, error) {
	properties := map[string]any{}
	mapping, err := s.mapper.ColumnToSearchMapping(c)
	if err != nil {
		if errors.As(err, &search.ErrTypeInvalid{}) {
			s.logger.Warn(err, "unknown column type", loglib.Fields{
				"column": map[string]any{
					"type": c.Type,
					"id":   c.GetColumnPgstreamID(tblPgstreamID),
				},
				"schema": schemaName,
				"table":  tblName,
			})
		} else {
			return nil, err
		}
	}

	if mapping != nil {
		properties[c.GetColumnPgstreamID(tblPgstreamID)] = mapping
		// add an alias for the original column name (use table.column
		// to avoid conflicts)
		properties[tblName+"."+c.Name] = map[string]any{
			"type": "alias",
			"path": c.GetColumnPgstreamID(tblPgstreamID),
		}
	}
	return properties, nil
}

func mapError(err error) error {
	if errors.As(err, &searchstore.RetryableError{}) {
		return fmt.Errorf("%w: %w", search.ErrRetriable, err)
	}
	if errors.As(err, &searchstore.ErrQueryInvalid{}) {
		return fmt.Errorf("%w: %w", search.ErrInvalidQuery, err)
	}
	return err
}
