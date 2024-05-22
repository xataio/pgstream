// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xataio/pgstream/internal/es"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"

	"github.com/rs/zerolog/log"
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

const (
	// OpenSearch has a limit of 512 bytes for the ID field. see here:
	// https://www.elastic.co/guide/en/elasticsearch/reference/7.10/mapping-id-field.html
	osIDFieldLengthLimit = 512
)

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
		if len(doc.ID) > osIDFieldLengthLimit {
			log.Error().
				Str("severity", "DATALOSS").
				Str("error", "ID is longer than 512 bytes").
				Str("id", doc.ID).
				Msg("opensearch store adapter: error processing document, skipping")
			continue
		}
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
	bulkIndex := &es.BulkIndex{
		Index:       indexName.Name(),
		ID:          doc.ID,
		Version:     doc.Version,
		VersionType: "external",
	}
	if doc.Delete {
		item.Delete = bulkIndex
	} else {
		item.Index = bulkIndex
	}
	return item
}

func (a *adapter) bulkItemToSearchDocErr(item es.BulkItem) search.DocumentError {
	doc := search.DocumentError{
		Document: search.Document{
			Data: item.Doc,
		},
		Error:  string(item.Error),
		Status: item.Status,
	}
	switch {
	case item.Index != nil:
		doc.Document.Schema = a.IndexToSchemaName(item.Index.Index)
		doc.Document.ID = item.Index.ID
		doc.Document.Version = item.Index.Version
	case item.Delete != nil:
		doc.Document.Schema = a.IndexToSchemaName(item.Delete.Index)
		doc.Document.ID = item.Delete.ID
		doc.Document.Version = item.Delete.Version
		doc.Document.Delete = true
	}
	return doc
}
