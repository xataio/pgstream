// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type mockAdapter struct {
	walEventToQueriesFn func(*wal.Event) ([]*query, error)
}

func (m *mockAdapter) walEventToQueries(_ context.Context, e *wal.Event) ([]*query, error) {
	return m.walEventToQueriesFn(e)
}

func (m *mockAdapter) close() error {
	return nil
}

type mockSchemaObserver struct {
	getGeneratedColumnNamesFn    func(ctx context.Context, schema, table string) ([]string, error)
	isMaterializedViewFn         func(schema, table string) bool
	updateGeneratedColumnNamesFn func(logEntry *schemalog.LogEntry)
	updateMaterializedViewsFn    func(logEntry *schemalog.LogEntry)
	closeFn                      func() error
}

func (m *mockSchemaObserver) getGeneratedColumnNames(ctx context.Context, schema, table string) ([]string, error) {
	return m.getGeneratedColumnNamesFn(ctx, schema, table)
}

func (m *mockSchemaObserver) isMaterializedView(schema, table string) bool {
	return m.isMaterializedViewFn(schema, table)
}

func (m *mockSchemaObserver) updateGeneratedColumnNames(logEntry *schemalog.LogEntry) {
	m.updateGeneratedColumnNamesFn(logEntry)
}

func (m *mockSchemaObserver) updateMaterializedViews(logEntry *schemalog.LogEntry) {
	m.updateMaterializedViewsFn(logEntry)
}

func (m *mockSchemaObserver) close() error {
	return m.closeFn()
}

type mockDDLAdapter struct {
	schemaLogToQueriesFn func(ctx context.Context, l *schemalog.LogEntry) ([]*query, error)
}

func (m *mockDDLAdapter) schemaLogToQueries(ctx context.Context, l *schemalog.LogEntry) ([]*query, error) {
	return m.schemaLogToQueriesFn(ctx, l)
}

type mockDMLAdapter struct {
	walDataToQueryFn func(d *wal.Data, generatedColumns []string) (*query, error)
}

func (m *mockDMLAdapter) walDataToQuery(d *wal.Data, generatedColumns []string) (*query, error) {
	return m.walDataToQueryFn(d, generatedColumns)
}
