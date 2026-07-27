// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

type mockAdapter struct {
	walEventToQueriesFn func(*wal.Event) ([]*query, error)
	walEventToMessageFn func(*wal.Event) (*walMessage, error)
}

func (m *mockAdapter) walEventToQueries(_ context.Context, e *wal.Event) ([]*query, error) {
	return m.walEventToQueriesFn(e)
}

func (m *mockAdapter) walEventToMessage(_ context.Context, e *wal.Event) (*walMessage, error) {
	if m.walEventToMessageFn != nil {
		return m.walEventToMessageFn(e)
	}
	return &walMessage{}, nil
}

func (m *mockAdapter) close() error {
	return nil
}

type mockSchemaObserver struct {
	getSchemaInfoFn      func(ctx context.Context, schema, table string) (schemaInfo, error)
	isMaterializedViewFn func(schema, table string) bool
	updateFn             func(ddlEvent *wal.DDLEvent)
	closeFn              func() error
}

func (m *mockSchemaObserver) getSchemaInfo(ctx context.Context, schema, table string) (schemaInfo, error) {
	if m.getSchemaInfoFn == nil {
		return schemaInfo{}, nil
	}
	return m.getSchemaInfoFn(ctx, schema, table)
}

func (m *mockSchemaObserver) isMaterializedView(ctx context.Context, schema, table string) bool {
	return m.isMaterializedViewFn(schema, table)
}

func (m *mockSchemaObserver) update(ddlEvent *wal.DDLEvent) {
	m.updateFn(ddlEvent)
}

func (m *mockSchemaObserver) close() error {
	return m.closeFn()
}

type mockDDLAdapter struct {
	walDataToQueriesFn func(ctx context.Context, d *wal.Data) ([]*query, error)
}

func (m *mockDDLAdapter) walDataToQueries(ctx context.Context, d *wal.Data) ([]*query, error) {
	return m.walDataToQueriesFn(ctx, d)
}

type mockDMLAdapter struct {
	walDataToQueriesFn func(d *wal.Data, schemaInfo schemaInfo) ([]*query, error)
}

func (m *mockDMLAdapter) walDataToQueries(d *wal.Data, schemaInfo schemaInfo) ([]*query, error) {
	return m.walDataToQueriesFn(d, schemaInfo)
}
