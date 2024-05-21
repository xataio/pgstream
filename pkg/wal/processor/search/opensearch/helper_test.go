// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"github.com/xataio/pgstream/internal/es"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

type mockAdapter struct {
	recordToLogEntryFn         func(map[string]any) (*schemalog.LogEntry, error)
	schemaNameToIndexFn        func(schemaName string) IndexName
	indexToSchemaNameFn        func(index string) string
	searchDocsToBulkItemsFn    func(docs []search.Document) []es.BulkItem
	bulkItemsToSearchDocErrsFn func(items []es.BulkItem) []search.DocumentError
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

func (m *mockAdapter) SearchDocsToBulkItems(docs []search.Document) []es.BulkItem {
	return m.searchDocsToBulkItemsFn(docs)
}

func (m *mockAdapter) BulkItemsToSearchDocErrs(items []es.BulkItem) []search.DocumentError {
	return m.bulkItemsToSearchDocErrsFn(items)
}

type mockMapper struct {
	columnToSearchMappingFn func(column schemalog.Column) (map[string]any, error)
	mapColumnValueFn        func(column schemalog.Column, value any) (any, error)
}

func (m *mockMapper) ColumnToSearchMapping(column schemalog.Column) (map[string]any, error) {
	return m.columnToSearchMappingFn(column)
}

func (m *mockMapper) MapColumnValue(column schemalog.Column, value any) (any, error) {
	return m.mapColumnValueFn(column, value)
}
