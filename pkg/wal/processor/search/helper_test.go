// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
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
	applySchemaDiffFn      func(ctx context.Context, diff *wal.SchemaDiff) error
	deleteSchemaFn         func(ctx context.Context, i uint, schemaName string) error
	deleteTableDocumentsFn func(ctx context.Context, schemaName string, tableIDs []string) error
	sendDocumentsFn        func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error)
	sendDocumentsCalls     uint
	deleteSchemaCalls      uint
}

func (m *mockStore) GetMapper() Mapper {
	return m.getMapperFn()
}

func (m *mockStore) ApplySchemaDiff(ctx context.Context, diff *wal.SchemaDiff) error {
	return m.applySchemaDiffFn(ctx, diff)
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
	testLSN        = 7773397064
)

var errTest = errors.New("oh noes")

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
			TablePgstreamID: testTableID,
			InternalColIDs:  []string{"col-1"},
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
		Version: testLSN,
		Data: map[string]any{
			"_table": testTableID,
			"col-2":  int64(0),
			"col-3":  "a",
		},
	}

	for _, opt := range opts {
		opt(doc)
	}
	return doc
}

func newTestCommitPosition() wal.CommitPosition {
	return wal.CommitPosition("test_topic/0/1")
}

func newTestWALDDLEvent() *wal.Event {
	testDDLEvent := newTestDDLEvent()
	testDDLEventJSON, _ := json.Marshal(testDDLEvent)

	return &wal.Event{
		Data: &wal.Data{
			Action:  wal.LogicalMessageAction,
			Prefix:  wal.DDLPrefix,
			Content: string(testDDLEventJSON),
		},
		CommitPosition: newTestCommitPosition(),
	}
}

func newTestDDLEvent() *wal.DDLEvent {
	return &wal.DDLEvent{
		DDL:        "CREATE TABLE test_schema.test_table (col-1 text PRIMARY KEY, col-2 integer);",
		SchemaName: testSchemaName,
		CommandTag: "CREATE TABLE",
		Objects: []wal.DDLObject{
			{
				Type:       "table",
				Identity:   "test_schema.test_table",
				Schema:     "test_schema",
				OID:        "123456",
				PgstreamID: testTableID,
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "col-1", Type: "text", Nullable: false, Generated: false, Unique: true},
					{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true, Generated: false, Unique: false},
				},
				PrimaryKeyColumns: []string{"col-1"},
			},
		},
	}
}

func newTestSchemaDiff() *wal.SchemaDiff {
	return &wal.SchemaDiff{
		SchemaName: "test_schema",
		TablesAdded: []wal.DDLObject{
			{
				Type:       "table",
				OID:        "123456",
				Identity:   "test_schema.test_table",
				Schema:     "test_schema",
				PgstreamID: testTableID,
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "col-1", Type: "text", Nullable: false, Generated: false, Unique: true},
					{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true, Generated: false, Unique: false},
				},
				PrimaryKeyColumns: []string{"col-1"},
			},
		},
	}
}
