// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xataio/pgstream/internal/es"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type Adapter interface {
	SchemaNameToIndex(schemaName string) IndexName
	IndexToSchemaName(index string) string
	SearchDocsToBulkItems(docs []search.Document) []es.BulkItem
	BulkItemsToSearchDocErrs(items []es.BulkItem) []search.DocumentError
	RecordToLogEntry(rec map[string]any) (*schemalog.LogEntry, error)
}

type adapter struct {
	marshaler   func(any) ([]byte, error)
	unmarshaler func([]byte, any) error
}

func newDefaultAdapter() *adapter {
	return &adapter{
		marshaler:   json.Marshal,
		unmarshaler: json.Unmarshal,
	}
}

func (a *adapter) SchemaNameToIndex(schemaName string) IndexName {
	return newDefaultIndexName(schemaName)
}

func (a *adapter) IndexToSchemaName(index string) string {
	return strings.TrimSuffix(index, "-1")
}

func (a *adapter) SearchDocsToBulkItems(docs []search.Document) []es.BulkItem {
	items := make([]es.BulkItem, 0, len(docs))
	for _, doc := range docs {
		items = append(items, a.searchDocToBulkItem(doc))
	}
	return items
}

func (a *adapter) BulkItemsToSearchDocErrs(items []es.BulkItem) []search.DocumentError {
	if items == nil {
		return nil
	}
	failedDocs := make([]search.DocumentError, 0, len(items))
	for _, item := range items {
		failedDocs = append(failedDocs, a.bulkItemToSearchDocErr(item))
	}
	return failedDocs
}

func (a *adapter) RecordToLogEntry(rec map[string]any) (*schemalog.LogEntry, error) {
	recBytes, err := a.marshaler(rec)
	if err != nil {
		return nil, fmt.Errorf("opensearch record to schemalog entry: failed to marshal document: %w", err)
	}

	var log schemalog.LogEntry
	if err := a.unmarshaler(recBytes, &log); err != nil {
		return nil, fmt.Errorf("opensearch record to schemalog entry: failed to unmarshal document: %w", err)
	}

	return &log, nil
}

func (a *adapter) searchDocToBulkItem(doc search.Document) es.BulkItem {
	indexName := a.SchemaNameToIndex(doc.Schema)
	item := es.BulkItem{
		Doc: doc.Data,
	}
	if doc.Deleted {
		item.Delete = &es.BulkIndex{
			Index:       indexName.Name(),
			ID:          doc.ID,
			Version:     doc.Version,
			VersionType: "external",
		}
	} else {
		item.Index = &es.BulkIndex{
			Index:       indexName.Name(),
			ID:          doc.ID,
			Version:     doc.Version,
			VersionType: "external",
		}
	}
	return item
}

func (a *adapter) bulkItemToSearchDocErr(item es.BulkItem) search.DocumentError {
	doc := search.DocumentError{
		Error:  item.Error,
		Status: item.Status,
	}
	switch {
	case item.Index != nil:
		doc.Schema = a.IndexToSchemaName(item.Index.Index)
		doc.ID = item.Index.ID
	case item.Delete != nil:
		doc.Schema = a.IndexToSchemaName(item.Delete.Index)
		doc.ID = item.Delete.ID
	}
	return doc
}
