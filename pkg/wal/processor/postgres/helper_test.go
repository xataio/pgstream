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
	getGeneratedColumnNamesFn func(ctx context.Context, schema, table string) (map[string]struct{}, error)
	getSequenceColumnsFn      func(ctx context.Context, schema, table string) (map[string]string, error)
	isMaterializedViewFn      func(schema, table string) bool
	updateFn                  func(logEntry *schemalog.LogEntry)
	closeFn                   func() error
}

func (m *mockSchemaObserver) getGeneratedColumnNames(ctx context.Context, schema, table string) (map[string]struct{}, error) {
	return m.getGeneratedColumnNamesFn(ctx, schema, table)
}

func (m *mockSchemaObserver) getSequenceColumns(ctx context.Context, schema, table string) (map[string]string, error) {
	return m.getSequenceColumnsFn(ctx, schema, table)
}

func (m *mockSchemaObserver) isMaterializedView(ctx context.Context, schema, table string) bool {
	return m.isMaterializedViewFn(schema, table)
}

func (m *mockSchemaObserver) update(logEntry *schemalog.LogEntry) {
	m.updateFn(logEntry)
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
	walDataToQueriesFn func(d *wal.Data, schemaInfo schemaInfo) ([]*query, error)
}

func (m *mockDMLAdapter) walDataToQueries(d *wal.Data, schemaInfo schemaInfo) ([]*query, error) {
	return m.walDataToQueriesFn(d, schemaInfo)
}
