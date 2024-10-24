// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/xid"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type mockAdapter struct {
	walEventToMsgFn func(*wal.Event) (*msg, error)
}

func (m *mockAdapter) walEventToMsg(e *wal.Event) (*msg, error) {
	return m.walEventToMsgFn(e)
}

type mockStore struct {
	getMapperFn            func() Mapper
	applySchemaChangeFn    func(ctx context.Context, le *schemalog.LogEntry) error
	deleteSchemaFn         func(ctx context.Context, i uint, schemaName string) error
	deleteTableDocumentsFn func(ctx context.Context, schemaName string, tableIDs []string) error
	sendDocumentsFn        func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error)
	sendDocumentsCalls     uint
	deleteSchemaCalls      uint
}

func (m *mockStore) GetMapper() Mapper {
	return m.getMapperFn()
}

func (m *mockStore) ApplySchemaChange(ctx context.Context, le *schemalog.LogEntry) error {
	return m.applySchemaChangeFn(ctx, le)
}

func (m *mockStore) DeleteSchema(ctx context.Context, schemaName string) error {
	m.deleteSchemaCalls++
	return m.deleteSchemaFn(ctx, m.deleteSchemaCalls, schemaName)
}

func (m *mockStore) DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) error {
	return m.deleteTableDocumentsFn(ctx, schemaName, tableIDs)
}

func (m *mockStore) SendDocuments(ctx context.Context, docs []Document) ([]DocumentError, error) {
	m.sendDocumentsCalls++
	return m.sendDocumentsFn(ctx, m.sendDocumentsCalls, docs)
}

const (
	testSchemaName = "test_schema"
	testTableName  = "test_table"
	testTableID    = "t1"
)

var errTest = errors.New("oh noes")

func newTestSchemaChangeEvent(action string, id xid.ID, now time.Time) *wal.Event {
	nowStr := now.Format("2006-01-02 15:04:05")
	return &wal.Event{
		Data: &wal.Data{
			Action: action,
			Schema: schemalog.SchemaName,
			Table:  schemalog.TableName,
			Columns: []wal.Column{
				{ID: "id", Name: "id", Type: "text", Value: id.String()},
				{ID: "version", Name: "version", Type: "integer", Value: 0},
				{ID: "schema_name", Name: "schema_name", Type: "text", Value: testSchemaName},
				{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
			},
		},
		CommitPosition: newTestCommitPosition(),
	}
}

func newTestDataEvent(action string) *wal.Event {
	cols := []wal.Column{
		{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
		{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
		{ID: "col-3", Name: "name", Type: "text", Value: "a"},
	}
	d := &wal.Data{
		Action: action,
		Schema: testSchemaName,
		Table:  testTableName,
		Metadata: wal.Metadata{
			TablePgstreamID:    testTableID,
			InternalColIDs:     []string{"col-1"},
			InternalColVersion: "col-2",
		},
	}

	if d.Action == "D" {
		d.Identity = cols
	} else {
		d.Columns = cols
	}

	return &wal.Event{
		Data:           d,
		CommitPosition: newTestCommitPosition(),
	}
}

type testDocOption func(*Document)

func withID(id string) testDocOption {
	return func(d *Document) {
		d.ID = id
	}
}

func newTestDocument(opts ...testDocOption) *Document {
	doc := &Document{
		Schema:  testSchemaName,
		ID:      fmt.Sprintf("%s_id-1", testTableID),
		Version: 0,
		Data: map[string]any{
			"_table": testTableID,
			"col-3":  "a",
		},
	}

	for _, opt := range opts {
		opt(doc)
	}
	return doc
}

func newTestLogEntry(id xid.ID, now time.Time) *schemalog.LogEntry {
	return &schemalog.LogEntry{
		ID:         id,
		Version:    0,
		SchemaName: testSchemaName,
		CreatedAt:  schemalog.NewSchemaCreatedAtTimestamp(now),
	}
}

func newTestCommitPosition() wal.CommitPosition {
	return wal.CommitPosition("test_topic/0/1")
}
