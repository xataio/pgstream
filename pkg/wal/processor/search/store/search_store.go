// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/internal/searchstore"
	elasticsearchstore "github.com/xataio/pgstream/internal/searchstore/elasticsearch"
	opensearchstore "github.com/xataio/pgstream/internal/searchstore/opensearch"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
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

func (s *Store) ApplySchemaChange(ctx context.Context, newEntry *schemalog.LogEntry) error {
	if newEntry == nil {
		return nil
	}
	existingLogEntry, err := s.getLastSchemaLogEntry(ctx, newEntry.SchemaName)
	if err != nil {
		// if there's no schemalog, this is a new schema and we need to create
		// it
		if errors.As(err, &search.ErrSchemaNotFound{}) {
			if err := s.ensureSchema(ctx, newEntry.SchemaName); err != nil {
				return fmt.Errorf("ensuring schema existence: %w", err)
			}
		} else {
			return fmt.Errorf("get latest schema: %w", err)
		}
	}

	// make sure the index and the mapping for the schema exist, and if it
	// doesn't, align it with latest schema log mapping. This check will allow
	// us to self recover in case of schema search index deletion
	if err := s.ensureSchemaMapping(ctx, newEntry.SchemaName, existingLogEntry); err != nil {
		return fmt.Errorf("ensuring schema mapping: %w", err)
	}

	// older schema should not be possible to receive
	// We compare version field rather than id because XID is not actually sortable.
	if existingLogEntry != nil && newEntry.Version <= existingLogEntry.Version {
		return search.ErrSchemaUpdateOutOfOrder{
			SchemaName:       newEntry.SchemaName,
			SchemaID:         newEntry.ID.String(),
			NewVersion:       int(newEntry.Version),
			NewCreatedAt:     newEntry.CreatedAt.Time,
			CurrentVersion:   int(existingLogEntry.Version),
			CurrentCreatedAt: existingLogEntry.CreatedAt.Time,
		}
	}

	changes := newEntry.Diff(existingLogEntry)
	// for now only log a warning when a reindexing is required, since the data
	// can still be indexed, old data will need a reindex to take into account
	// the new identity for search to be effective
	//
	// TODO: in the future, we will trigger a reindex automatically in this
	// situation
	for _, tbl := range changes.PrimaryKeyChange {
		s.logger.Warn(nil, fmt.Sprintf("primary key identity column changed for table %s, reindexing required", tbl))
	}
	for _, tbl := range changes.UniqueNotNullChange {
		s.logger.Warn(nil, fmt.Sprintf("unique not null identity column changed for table %s, reindexing required", tbl))
	}

	if err := s.updateMapping(ctx, newEntry.SchemaName, newEntry, changes); err != nil {
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

	if exists {
		if err := s.client.DeleteIndex(ctx, []string{index.NameWithVersion()}); err != nil {
			return mapError(err)
		}
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

// getLastSchemaLogEntry will return the last version of the schemalog for the
// schema on input. A nil LogEntry will be returned when there's no existing
// associated logs
func (s *Store) getLastSchemaLogEntry(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
	query := searchstore.QueryBody{
		Query: &searchstore.Query{
			Bool: &searchstore.BoolFilter{
				Filter: []searchstore.Condition{
					{
						Term: map[string]any{
							"schema_name": schemaName,
						},
					},
				},
			},
		},
	}

	bodyJSON, err := s.marshaler(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to JSON: %+v, %w", query, err)
	}

	res, err := s.client.Search(ctx, &searchstore.SearchRequest{
		Index: searchstore.Ptr(schemalogIndexName),
		Size:  searchstore.Ptr(1),
		Sort:  searchstore.Ptr("version:desc"),
		Query: bytes.NewBuffer(bodyJSON),
	})
	if err != nil {
		if errors.Is(err, searchstore.ErrResourceNotFound) {
			s.logger.Warn(err, "index not found, trying to create it", loglib.Fields{"index": schemalogIndexName})
			// Create the pgstream index if it was not found.
			err = s.createSchemaLogIndex(ctx, schemalogIndexName)
			if err != nil {
				return nil, mapError(err)
			}
			return nil, search.ErrSchemaNotFound{SchemaName: schemaName}
		}
		return nil, fmt.Errorf("get latest schema, failed to search: %w", mapError(err))
	}

	if len(res.Hits.Hits) == 0 {
		return nil, search.ErrSchemaNotFound{SchemaName: schemaName}
	}

	return s.adapter.RecordToLogEntry(res.Hits.Hits[0].Source)
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

func (s *Store) updateMapping(ctx context.Context, schemaName string, logEntry *schemalog.LogEntry, diff *schemalog.SchemaDiff) error {
	index := s.indexNameAdapter.SchemaNameToIndex(schemaName)
	if diff != nil {
		if err := s.updateMappingAddNewColumns(ctx, index, diff.ColumnsToAdd); err != nil {
			return fmt.Errorf("failed to add new columns: %w", mapError(err))
		}

		if len(diff.TablesToRemove) > 0 {
			tableIDs := make([]string, 0, len(diff.TablesToRemove))
			for _, table := range diff.TablesToRemove {
				tableIDs = append(tableIDs, table.PgstreamID)
			}
			if err := s.deleteTableDocuments(ctx, index, tableIDs); err != nil {
				return fmt.Errorf("failed to delete table documents: %w", mapError(err))
			}
		}
	}

	if err := s.insertNewSchemaLog(ctx, logEntry); err != nil {
		return fmt.Errorf("failed to insert new schema log: %w", mapError(err))
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

func (s *Store) createSchemaLogIndex(ctx context.Context, name string) error {
	exists, err := s.client.IndexExists(ctx, schemalogIndexName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.client.CreateIndex(ctx, name, map[string]any{
		"mappings": map[string]any{
			"dynamic": "strict",
			"properties": map[string]any{
				"id": map[string]any{
					"type": "keyword",
				},
				"version": map[string]any{
					"type": "long",
				},
				"schema_name": map[string]any{
					"type": "keyword",
				},
				"schema": map[string]any{
					// we store json but opt to use `text` as `flat_object` is not ready yet
					"type": "text",
				},
				"created_at": map[string]any{
					"type":   "date",
					"format": "yyyy-MM-dd HH:mm:ss.SSSSSS",
				},
				"acked": map[string]any{
					"type": "boolean",
				},
			},
		},
	})
}

func (s *Store) updateMappingAddNewColumns(ctx context.Context, indexName IndexName, newColumns []schemalog.Column) error {
	if len(newColumns) == 0 {
		return nil
	}

	properties := map[string]any{}

	for _, c := range newColumns {
		mapping, err := s.mapper.ColumnToSearchMapping(c)
		if err != nil {
			if errors.As(err, &search.ErrTypeInvalid{}) {
				s.logger.Warn(err, "unknown column type", loglib.Fields{
					"column": map[string]any{
						"type": c.DataType,
						"id":   c.PgstreamID,
					},
					"schema": indexName.SchemaName(),
				})
			} else {
				return fmt.Errorf("failed to convert column to search mapping: %w", err)
			}
		}

		if mapping != nil {
			properties[c.PgstreamID] = mapping
		}
	}

	return s.client.PutIndexMappings(ctx, indexName.Name(), map[string]any{
		"properties": properties,
	})
}

func (s *Store) insertNewSchemaLog(ctx context.Context, m *schemalog.LogEntry) error {
	logBytes, err := s.marshaler(m)
	if err != nil {
		return fmt.Errorf("insert schema log, failed to marshal search doc: %w", err)
	}

	err = s.client.IndexWithID(ctx, &searchstore.IndexWithIDRequest{
		Index:   schemalogIndexName,
		ID:      m.ID.String(),
		Body:    logBytes,
		Refresh: "true", // necessary to make sure all reads use this schema right away
	})
	if err != nil {
		return fmt.Errorf("insert schema log: %w", err)
	}

	return nil
}

func (s *Store) ensureSchema(ctx context.Context, schemaName string) error {
	return s.ensureSchemaMapping(ctx, schemaName, nil)
}

func (s *Store) ensureSchemaMapping(ctx context.Context, schemaName string, metadata *schemalog.LogEntry) error {
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
		// if the schema didn't exist, but there's a log entry in the schemalog,
		// we need to reset it to the latest known mapping
		if metadata != nil && !metadata.IsEmpty() {
			if err := s.updateMapping(ctx, schemaName, metadata, metadata.Diff(&schemalog.LogEntry{})); err != nil {
				return fmt.Errorf("updating mapping for missing schema: %w", err)
			}
		}
	}
	return nil
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
