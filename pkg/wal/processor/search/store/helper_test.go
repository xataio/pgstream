// SPDX-License-Identifier: Apache-2.0

package store

import (
	"github.com/xataio/pgstream/internal/searchstore"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type mockAdapter struct {
	recordToLogEntryFn         func(map[string]any) (*schemalog.LogEntry, error)
	schemaNameToIndexFn        func(schemaName string) IndexName
	indexToSchemaNameFn        func(index string) string
	searchDocToBulkItemFn      func(docs search.Document) searchstore.BulkItem
	bulkItemsToSearchDocErrsFn func(items []searchstore.BulkItem) []search.DocumentError
}

func (m *mockAdapter) RecordToLogEntry(rec map[string]any) (*schemalog.LogEntry, error) {
	return m.recordToLogEntryFn(rec)
}

func (m *mockAdapter) SchemaNameToIndex(schemaName string) IndexName {
	return m.schemaNameToIndexFn(schemaName)
}

func (m *mockAdapter) IndexToSchemaName(index string) string {
	return m.indexToSchemaNameFn(index)
}

func (m *mockAdapter) SearchDocToBulkItem(docs search.Document) searchstore.BulkItem {
	return m.searchDocToBulkItemFn(docs)
}

func (m *mockAdapter) BulkItemsToSearchDocErrs(items []searchstore.BulkItem) []search.DocumentError {
	return m.bulkItemsToSearchDocErrsFn(items)
}
