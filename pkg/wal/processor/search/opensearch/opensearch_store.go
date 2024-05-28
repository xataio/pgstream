// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/xataio/pgstream/internal/es"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type Store struct {
	client    es.SearchClient
	mapper    search.Mapper
	adapter   Adapter
	marshaler func(any) ([]byte, error)
}

type Config struct {
	URL string
}

const (
	openSearchDefaultANNEngine      = "nmslib"
	openSearchDefaultM              = 48
	openSearchDefaultEFConstruction = 256
	openSearchDefaultEFSearch       = 100

	schemalogIndexName = "pgstream"
)

func NewStore(cfg Config) (*Store, error) {
	os, err := es.NewClient(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("create elasticsearch client: %w", err)
	}
	return NewStoreWithClient(os), nil
}

func NewStoreWithClient(client es.SearchClient) *Store {
	return &Store{
		client:    client,
		adapter:   newDefaultAdapter(),
		mapper:    NewPostgresMapper(),
		marshaler: json.Marshal,
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
	// us to self recover in case of schema OS index deletion
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
	if err := s.updateMapping(ctx, newEntry.SchemaName, newEntry, changes); err != nil {
		return fmt.Errorf("update mapping for schema: %w", err)
	}
	return nil
}

func (s *Store) SendDocuments(ctx context.Context, docs []search.Document) ([]search.DocumentError, error) {
	failed, err := s.client.SendBulkRequest(ctx, s.adapter.SearchDocsToBulkItems(docs))
	if err != nil {
		return nil, mapError(err)
	}

	return s.adapter.BulkItemsToSearchDocErrs(failed), nil
}

func (s *Store) DeleteSchema(ctx context.Context, schemaName string) error {
	index := s.adapter.SchemaNameToIndex(schemaName)
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
	if err := s.client.DeleteByQuery(ctx, &es.DeleteByQueryRequest{
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
	index := s.adapter.SchemaNameToIndex(schemaName)
	if err := s.deleteTableDocuments(ctx, index, tableIDs); err != nil {
		return mapError(err)
	}
	return nil
}

// getLastSchemaLogEntry will return the last version of the schemalog for the
// schema on input. A nil LogEntry will be returned when there's no existing
// associated logs
func (s *Store) getLastSchemaLogEntry(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
	query := es.QueryBody{
		Query: &es.Query{
			Bool: &es.BoolFilter{
				Filter: []es.Condition{
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

	res, err := s.client.Search(ctx, &es.SearchRequest{
		Index: es.Ptr(schemalogIndexName),
		Size:  es.Ptr(1),
		Sort:  es.Ptr("version:desc"),
		Query: bytes.NewBuffer(bodyJSON),
	})
	if err != nil {
		if errors.Is(err, es.ErrResourceNotFound) {
			log.Warn().Msgf("[%s]: index not found: %v. Trying to create it", schemalogIndexName, err)
			// Create the pgstream index if it was not found.
			err = s.createSchemaLogIndex(ctx, schemalogIndexName)
			if err != nil {
				return nil, mapError(err)
			}
			return nil, search.ErrSchemaNotFound{SchemaName: schemaName}
		}
		return nil, fmt.Errorf("get latest schema, failed to search os: %w", mapError(err))
	}

	if len(res.Hits.Hits) == 0 {
		return nil, search.ErrSchemaNotFound{SchemaName: schemaName}
	}

	return s.adapter.RecordToLogEntry(res.Hits.Hits[0].Source)
}

func (s *Store) schemaExists(ctx context.Context, schemaName string) (bool, error) {
	indexName := s.adapter.SchemaNameToIndex(schemaName)
	exists, err := s.client.IndexExists(ctx, indexName.NameWithVersion())
	if err != nil {
		return false, mapError(err)
	}
	return exists, nil
}

func (s *Store) createSchema(ctx context.Context, schemaName string) error {
	index := s.adapter.SchemaNameToIndex(schemaName)
	err := s.client.CreateIndex(ctx, index.NameWithVersion(), map[string]any{
		"mappings": map[string]any{
			"dynamic": "strict",
			"properties": map[string]any{
				"_table": map[string]any{
					"type": "keyword",
				},
			},
		},
		"settings": map[string]any{
			"number_of_shards":                 1,
			"number_of_replicas":               1,
			"index.mapping.total_fields.limit": 2000,
			"index.knn":                        true,
			"knn.algo_param.ef_search":         openSearchDefaultEFSearch,
		},
	})
	if err != nil {
		if errors.As(err, &es.ErrResourceAlreadyExists{}) {
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
	index := s.adapter.SchemaNameToIndex(schemaName)
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

	req := &es.DeleteByQueryRequest{
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
				log.Warn().Dict("column", zerolog.Dict().Str("type", c.DataType).Str("id", c.PgstreamID)).
					Str("schema", indexName.SchemaName()).
					Msgf("unknown column type: %v", err)
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
	logBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("insert schema log, failed to marshal es doc: %w", err)
	}

	err = s.client.IndexWithID(ctx, &es.IndexWithIDRequest{
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
	if errors.As(err, &es.RetryableError{}) {
		return fmt.Errorf("%w: %v", search.ErrRetriable, err.Error())
	}
	return err
}
