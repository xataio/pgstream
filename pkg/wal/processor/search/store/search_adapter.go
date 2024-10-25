// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/internal/searchstore"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

// Adapter converts from/to search types and opensearch types
type SearchAdapter interface {
	SearchDocToBulkItem(docs search.Document) searchstore.BulkItem
	BulkItemsToSearchDocErrs(items []searchstore.BulkItem) []search.DocumentError
	RecordToLogEntry(rec map[string]any) (*schemalog.LogEntry, error)
}

type adapter struct {
	indexNameAdapter IndexNameAdapter
	marshaler        func(any) ([]byte, error)
	unmarshaler      func([]byte, any) error
}

func newDefaultAdapter(indexNameAdapter IndexNameAdapter) *adapter {
	return &adapter{
		indexNameAdapter: indexNameAdapter,
		marshaler:        json.Marshal,
		unmarshaler:      json.Unmarshal,
	}
}

func (a *adapter) SearchDocToBulkItem(doc search.Document) searchstore.BulkItem {
	indexName := a.indexNameAdapter.SchemaNameToIndex(doc.Schema)
	item := searchstore.BulkItem{
		Doc: doc.Data,
	}
	bulkIndex := &searchstore.BulkIndex{
		Index:       indexName.Name(),
		ID:          doc.ID,
		Version:     &doc.Version,
		VersionType: "external",
	}
	if doc.Delete {
		item.Delete = bulkIndex
	} else {
		item.Index = bulkIndex
	}
	return item
}

func (a *adapter) BulkItemsToSearchDocErrs(items []searchstore.BulkItem) []search.DocumentError {
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

func (a *adapter) bulkItemToSearchDocErr(item searchstore.BulkItem) search.DocumentError {
	doc := search.DocumentError{
		Document: search.Document{
			Data: item.Doc,
		},
		Error: string(item.Error),
	}
	switch {
	case item.Index != nil:
		doc.Document.Schema = a.indexNameAdapter.IndexToSchemaName(item.Index.Index)
		doc.Document.ID = item.Index.ID
		if item.Index.Version != nil {
			doc.Document.Version = *item.Index.Version
		}
	case item.Delete != nil:
		doc.Document.Schema = a.indexNameAdapter.IndexToSchemaName(item.Delete.Index)
		doc.Document.ID = item.Delete.ID
		if item.Delete.Version != nil {
			doc.Document.Version = *item.Delete.Version
		}
		doc.Document.Delete = true
	}

	doc.Severity = a.parseSeverity(item.Status, doc.Document.Delete)

	return doc
}

func (a *adapter) parseSeverity(status int, delete bool) search.Severity {
	switch status {
	case 400: // 400 means that the document is invalid. We drop it.
		return search.SeverityDataLoss
	case 409: // ignore, likely event out of order
		return search.SeverityIgnored
	case 404:
		if delete { // ignore, likely event out of order
			return search.SeverityIgnored
		}
		return search.SeverityDataLoss
	case 429: // retry events, likely search store overloaded
		return search.SeverityRetriable
	default:
		if status >= 500 {
			// internal server error, just retry until opensearch can tell us
			// what the problem is
			return search.SeverityRetriable
		}
		// maybe DLQ, for now just log
		return search.SeverityDataLoss
	}
}
