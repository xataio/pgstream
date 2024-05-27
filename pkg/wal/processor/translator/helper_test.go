// SPDX-License-Identifier: Apache-2.0

package translator

import (
	"errors"
	"fmt"
	"time"

	"github.com/rs/xid"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

const (
	testSchemaName = "test_schema"
	testTableName  = "test_table"
	testTableID    = "t1"
)

var (
	testSchemaID = xid.New()
	errTest      = errors.New("oh noes")
	now          = time.Now()
)

func newTestLogEntry() *schemalog.LogEntry {
	return &schemalog.LogEntry{
		ID:         testSchemaID,
		Version:    0,
		SchemaName: testSchemaName,
		CreatedAt:  schemalog.NewSchemaCreatedAtTimestamp(now),
		Schema: schemalog.Schema{
			Tables: []schemalog.Table{
				{
					Name:       testTableName,
					PgstreamID: testTableID,
					Columns: []schemalog.Column{
						{Name: "col-1", DataType: "text", PgstreamID: fmt.Sprintf("%s_col-1", testTableID)},
						{Name: "col-2", DataType: "integer", PgstreamID: fmt.Sprintf("%s_col-2", testTableID)},
					},
				},
			},
		},
	}
}

func newTestSchemaChangeEvent(action string) *wal.Data {
	nowStr := now.Format("2006-01-02 15:04:05")
	return &wal.Data{
		Action: action,
		Schema: schemalog.SchemaName,
		Table:  schemalog.TableName,
		Columns: []wal.Column{
			{ID: "id", Name: "id", Type: "text", Value: testSchemaID.String()},
			{ID: "version", Name: "version", Type: "integer", Value: 0},
			{ID: "schema_name", Name: "schema_name", Type: "text", Value: testSchemaName},
			{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
		},
	}
}

func newTestDataEvent(action string) *wal.Data {
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

	return d
}

func newTestDataEventWithMetadata(action string) *wal.Data {
	d := newTestDataEvent(action)
	d.Columns = []wal.Column{
		{ID: fmt.Sprintf("%s_col-1", testTableID), Name: "col-1", Type: "text", Value: "id-1"},
		{ID: fmt.Sprintf("%s_col-2", testTableID), Name: "col-2", Type: "integer", Value: int64(0)},
	}
	d.Metadata = wal.Metadata{
		SchemaID:           testSchemaID,
		TablePgstreamID:    testTableID,
		InternalColID:      fmt.Sprintf("%s_col-1", testTableID),
		InternalColVersion: fmt.Sprintf("%s_col-2", testTableID),
	}
	return d
}
