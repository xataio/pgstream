// SPDX-License-Identifier: Apache-2.0

package injector

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/pkg/wal"
)

const (
	testSchemaName = "test_schema"
	testTableName  = "test_table"
	testTableID    = "t1"
)

var errTest = errors.New("oh noes")

func newTestWALDDLEvent() *wal.Event {
	testDDLEvent := newTestDDLEvent()
	testDDLEventJSON, _ := json.Marshal(testDDLEvent)

	return &wal.Event{
		Data: &wal.Data{
			Action:  wal.LogicalMessageAction,
			Prefix:  wal.DDLPrefix,
			Content: string(testDDLEventJSON),
		},
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

func newTestDataEvent(action string) *wal.Event {
	cols := []wal.Column{
		{ID: "col-1", Name: "col-1", Type: "text", Value: "id-1"},
		{ID: "col-2", Name: "col-2", Type: "integer", Value: int64(0)},
	}
	d := &wal.Data{
		Action: action,
		Schema: testSchemaName,
		Table:  testTableName,
	}

	if d.Action == "D" {
		d.Identity = cols
	} else {
		d.Columns = cols
	}

	return &wal.Event{
		Data: d,
	}
}

func newTestDataEventWithMetadata(action string) *wal.Event {
	d := newTestDataEvent(action)
	d.Data.Columns = []wal.Column{
		{ID: fmt.Sprintf("%s-1", testTableID), Name: "col-1", Type: "text", Value: "id-1"},
		{ID: fmt.Sprintf("%s-2", testTableID), Name: "col-2", Type: "integer", Value: int64(0)},
	}
	d.Data.Metadata = wal.Metadata{
		TablePgstreamID: testTableID,
		InternalColIDs:  []string{fmt.Sprintf("%s-1", testTableID)},
	}
	return d
}

func newTestTableObject() *wal.DDLObject {
	return &wal.DDLObject{
		Type:       "table",
		Identity:   fmt.Sprintf("%s.%s", testSchemaName, testTableName),
		Schema:     testSchemaName,
		OID:        "123456",
		PgstreamID: testTableID,
		Columns: []wal.DDLColumn{
			{Attnum: 1, Name: "col-1", Type: "text", Nullable: false, Generated: false, Unique: true},
			{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true, Generated: false, Unique: false},
		},
		PrimaryKeyColumns: []string{"col-1"},
	}
}

func newTestTableObjectNoPK() *wal.DDLObject {
	return &wal.DDLObject{
		Type:       "table",
		Identity:   fmt.Sprintf("%s.%s", testSchemaName, testTableName),
		Schema:     testSchemaName,
		OID:        "123456",
		PgstreamID: testTableID,
		Columns: []wal.DDLColumn{
			{Attnum: 1, Name: "col-1", Type: "text", Nullable: false, Generated: false, Unique: false},
			{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true, Generated: false, Unique: false},
		},
		PrimaryKeyColumns: []string{},
	}
}

func newTestWALDDLDropEvent() *wal.Event {
	testDDLEvent := &wal.DDLEvent{
		DDL:        "DROP TABLE test_schema.test_table;",
		SchemaName: testSchemaName,
		CommandTag: "DROP TABLE",
		Objects: []wal.DDLObject{
			{
				Type:       "table",
				Identity:   "test_schema.test_table",
				Schema:     "test_schema",
				OID:        "123456",
				PgstreamID: testTableID,
			},
		},
	}
	testDDLEventJSON, _ := json.Marshal(testDDLEvent)

	return &wal.Event{
		Data: &wal.Data{
			Action:  wal.LogicalMessageAction,
			Prefix:  wal.DDLPrefix,
			Content: string(testDDLEventJSON),
		},
	}
}
