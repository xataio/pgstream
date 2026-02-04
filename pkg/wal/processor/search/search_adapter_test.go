// SPDX-License-Identifier: Apache-2.0

package search

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	searchmocks "github.com/xataio/pgstream/pkg/wal/processor/search/mocks"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationmocks "github.com/xataio/pgstream/pkg/wal/replication/mocks"
)

func TestAdapter_walEventToMsg(t *testing.T) {
	t.Parallel()

	testDocBytes := []byte("test-doc")

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column *wal.Column) (any, error) { return column.Value, nil },
	}

	testLSNParser := &replicationmocks.LSNParser{
		FromStringFn: func(s string) (replication.LSN, error) { return replication.LSN(7773397064), nil },
	}

	tests := []struct {
		name                 string
		event                *wal.Event
		marshaler            func(any) ([]byte, error)
		mapper               Mapper
		walDataToDDLEvent    func(*wal.Data) (*wal.DDLEvent, error)
		ddlEventToSchemaDiff func(*wal.DDLEvent) (*wal.SchemaDiff, error)

		wantMsg *msg
		wantErr error
	}{
		{
			name: "ok - keep alive",
			event: &wal.Event{
				CommitPosition: newTestCommitPosition(),
			},

			wantMsg: &msg{},
			wantErr: nil,
		},
		{
			name:  "ok - ddl event",
			event: newTestWALDDLEvent(),

			wantMsg: &msg{
				schemaDiff: newTestSchemaDiff(),
			},
			wantErr: nil,
		},
		{
			name:      "ok - data event",
			event:     newTestDataEvent("I"),
			marshaler: func(a any) ([]byte, error) { return testDocBytes, nil },

			wantMsg: &msg{
				write:     newTestDocument(),
				bytesSize: len(testDocBytes),
			},
			wantErr: nil,
		},
		{
			name:  "ok - truncate event",
			event: newTestDataEvent("T"),

			wantMsg: &msg{
				truncate: &truncateItem{
					schemaName: testSchemaName,
					tableID:    testTableID,
				},
				bytesSize: len(testSchemaName) + len(testTableID),
			},
			wantErr: nil,
		},
		{
			name:  "ok - skipped action data events",
			event: newTestDataEvent("B"),

			wantMsg: nil,
			wantErr: nil,
		},
		{
			name:      "error - data event document size",
			event:     newTestDataEvent("I"),
			marshaler: func(a any) ([]byte, error) { return nil, errTest },

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name:      "error - data event to document",
			event:     newTestDataEvent("I"),
			marshaler: func(a any) ([]byte, error) { return testDocBytes, nil },
			mapper: &searchmocks.Mapper{
				MapColumnValueFn: func(column *wal.Column) (any, error) { return nil, errTest },
			},

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name:              "error - wal to ddl event",
			event:             newTestWALDDLEvent(),
			walDataToDDLEvent: func(d *wal.Data) (*wal.DDLEvent, error) { return nil, errTest },

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name:                 "error - ddl event to schema diff",
			event:                newTestWALDDLEvent(),
			ddlEventToSchemaDiff: func(d *wal.DDLEvent) (*wal.SchemaDiff, error) { return nil, errTest },

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name: "error - ddl event to schema diff",
			event: func() *wal.Event {
				d := newTestDataEvent("I")
				d.Data.Metadata = wal.Metadata{}
				return d
			}(),
			marshaler: func(a any) ([]byte, error) { return testDocBytes, nil },

			wantMsg: nil,
			wantErr: errMetadataMissing,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := newAdapter(noopMapper, testLSNParser, nil)

			if tc.marshaler != nil {
				a.marshaler = tc.marshaler
			}
			if tc.mapper != nil {
				a.mapper = tc.mapper
			}

			if tc.walDataToDDLEvent != nil {
				a.walDataToDDLEvent = tc.walDataToDDLEvent
			}

			if tc.ddlEventToSchemaDiff != nil {
				a.ddlEventToSchemaDiff = tc.ddlEventToSchemaDiff
			}

			item, err := a.walEventToMsg(tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantMsg, item)
		})
	}
}

func TestAdapter_walDataToDocument(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"

	errTest := errors.New("oh noes")

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column *wal.Column) (any, error) { return column.Value, nil },
	}

	testLSNParser := &replicationmocks.LSNParser{
		FromStringFn: func(s string) (replication.LSN, error) { return replication.LSN(7773397064), nil },
	}

	tests := []struct {
		name   string
		mapper Mapper
		data   *wal.Data

		wantDoc *Document
		wantErr error
	}{
		{
			name:   "ok - insert event",
			mapper: noopMapper,
			data:   newTestDataEvent("I").Data,

			wantDoc: newTestDocument(),
			wantErr: nil,
		},
		{
			name:   "ok - insert event with identity columns",
			mapper: noopMapper,
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Identity = []wal.Column{
					{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
					{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
					{ID: "col-4", Name: "toast", Type: "text", Value: "very-long-value"},
				}
				return d
			}(),
			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: testLSN,
				Schema:  testSchema,
				Data: map[string]any{
					"col-2":  int64(0),
					"col-3":  "a",
					"col-4":  "very-long-value",
					"_table": testTableID,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - insert event with identity columns and invalid type",
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Identity = []wal.Column{
					{ID: "col-4", Name: "toast", Type: "text", Value: "very-long-value"},
				}
				return d
			}(),
			mapper: &searchmocks.Mapper{
				MapColumnValueFn: func(column *wal.Column) (any, error) {
					if column.Name == "toast" {
						return nil, ErrTypeInvalid{Input: "toast"}
					}
					return column.Value, nil
				},
			},

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: testLSN,
				Schema:  testSchema,
				Data: map[string]any{
					"col-2":  int64(0),
					"col-3":  "a",
					"_table": testTableID,
				},
			},
			wantErr: nil,
		},
		{
			name:   "ok - delete event",
			mapper: noopMapper,
			data:   newTestDataEvent("D").Data,

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: testLSN + 1,
				Schema:  testSchema,
				Data: map[string]any{
					"col-2":  int64(0),
					"col-3":  "a",
					"_table": testTableID,
				},
				Delete: true,
			},
			wantErr: nil,
		},
		{
			name:   "error - parsing columns with insert event",
			mapper: noopMapper,
			data: &wal.Data{
				Action: "I",
				Schema: "test_schema",
				Table:  "test_table",
				Columns: []wal.Column{
					{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
					{ID: "col-3", Name: "name", Type: "text", Value: "a"},
				},
				Metadata: wal.Metadata{
					TablePgstreamID: testTableID,
					InternalColIDs:  []string{"col-1"},
				},
			},

			wantDoc: nil,
			wantErr: processor.ErrIDNotFound,
		},
		{
			name:   "error - parsing columns with delete event",
			mapper: noopMapper,
			data: &wal.Data{
				Action: "D",
				Schema: "test_schema",
				Table:  "test_table",
				Identity: []wal.Column{
					{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
					{ID: "col-3", Name: "name", Type: "text", Value: "a"},
				},
				Metadata: wal.Metadata{
					TablePgstreamID: testTableID,
					InternalColIDs:  []string{"col-1"},
				},
			},

			wantDoc: nil,
			wantErr: processor.ErrIDNotFound,
		},
		{
			name: "error - insert event with identity columns",
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Identity = []wal.Column{
					{ID: "col-4", Name: "toast", Type: "text", Value: "very-long-value"},
				}
				return d
			}(),
			mapper: &searchmocks.Mapper{
				MapColumnValueFn: func(column *wal.Column) (any, error) {
					if column.Name == "toast" {
						return nil, errTest
					}
					return column.Value, nil
				},
			},

			wantDoc: nil,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := newAdapter(tc.mapper, testLSNParser, nil)
			doc, err := a.walDataToDocument(tc.data)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantDoc, doc)
		})
	}
}

func TestAdapter_parseColumns(t *testing.T) {
	t.Parallel()

	testColumns := []wal.Column{
		{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
		{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
		{ID: "col-3", Name: "name", Type: "text", Value: "a"},
	}
	testMetadata := wal.Metadata{
		TablePgstreamID: testTableID,
		InternalColIDs:  []string{"col-1"},
	}

	testLSNStr := "1/CF54A048"
	testLSN := 7773397064
	testLSNParser := &replicationmocks.LSNParser{
		FromStringFn: func(s string) (replication.LSN, error) { return replication.LSN(testLSN), nil },
	}

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column *wal.Column) (any, error) { return column.Value, nil },
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name     string
		columns  []wal.Column
		metadata wal.Metadata
		mapper   Mapper
		parser   replication.LSNParser

		wantDoc *Document
		wantErr error
	}{
		{
			name:     "ok",
			columns:  testColumns,
			metadata: testMetadata,
			mapper:   noopMapper,

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: testLSN,
				Data: map[string]any{
					"col-2":  int64(0),
					"col-3":  "a",
					"_table": testTableID,
				},
			},
			wantErr: nil,
		},
		{
			name:     "ok - skip column with invalid type",
			columns:  testColumns,
			metadata: testMetadata,
			mapper: &searchmocks.Mapper{
				MapColumnValueFn: func(column *wal.Column) (any, error) {
					if column.Name == "name" {
						return nil, ErrTypeInvalid{}
					}
					return column.Value, nil
				},
			},

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: testLSN,
				Data: map[string]any{
					"col-2":  int64(0),
					"_table": testTableID,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - id not found",
			columns: []wal.Column{
				{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
				{ID: "col-3", Name: "name", Type: "text", Value: "a"},
			},
			metadata: testMetadata,
			mapper:   noopMapper,

			wantDoc: nil,
			wantErr: processor.ErrIDNotFound,
		},
		{
			name: "error - invalid id value",
			columns: []wal.Column{
				{ID: "col-1", Name: "id", Type: "text", Value: nil},
				{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
			},
			metadata: testMetadata,
			mapper:   noopMapper,

			wantDoc: nil,
			wantErr: errNilIDValue,
		},
		{
			name:     "error - mapping column value",
			columns:  testColumns,
			metadata: testMetadata,
			mapper: &searchmocks.Mapper{
				MapColumnValueFn: func(column *wal.Column) (any, error) {
					if column.Name == "name" {
						return nil, errTest
					}
					return column.Value, nil
				},
			},

			wantDoc: nil,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := newAdapter(tc.mapper, testLSNParser, nil)
			if tc.parser != nil {
				a.lsnParser = tc.parser
			}

			doc, err := a.parseColumns(tc.columns, tc.metadata, testLSNStr)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantDoc, doc)
		})
	}
}

func TestAdapter_parseIDColumns(t *testing.T) {
	t.Parallel()

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column *wal.Column) (any, error) { return column.Value, nil },
	}

	newDoc := func(id string) *Document {
		return &Document{
			ID:   id,
			Data: make(map[string]any),
		}
	}
	testUUID := "99d4f38c-770a-460e-b0ac-419fbc3d5636"

	testTable := "test-table"
	tests := []struct {
		name      string
		idColumns []wal.Column

		wantDoc *Document
		wantErr error
	}{
		{
			name: "ok - string",
			idColumns: []wal.Column{
				{ID: "col-1", Name: "id-1", Value: "id1"},
			},
			wantDoc: newDoc(fmt.Sprintf("%s_id1", testTable)),
			wantErr: nil,
		},
		{
			name: "ok - int32",
			idColumns: []wal.Column{
				{Name: "id-1", Value: int32(1)},
			},
			wantDoc: newDoc(fmt.Sprintf("%s_1", testTable)),
			wantErr: nil,
		},
		{
			name: "ok - int64",
			idColumns: []wal.Column{
				{Name: "id-1", Value: int64(1)},
			},
			wantDoc: newDoc(fmt.Sprintf("%s_1", testTable)),
			wantErr: nil,
		},
		{
			name: "ok - float64",
			idColumns: []wal.Column{
				{Name: "id-1", Value: float64(1.0)},
			},
			wantDoc: newDoc(fmt.Sprintf("%s_1", testTable)),
			wantErr: nil,
		},
		{
			name: "ok - [16]uint8 (UUID)",
			idColumns: []wal.Column{
				{Name: "id-1", Value: [16]uint8{153, 212, 243, 140, 119, 10, 70, 14, 176, 172, 65, 159, 188, 61, 86, 54}},
			},
			wantDoc: newDoc(fmt.Sprintf("%s_%s", testTable, testUUID)),
			wantErr: nil,
		},
		{
			name: "ok - multiple id columns",
			idColumns: []wal.Column{
				{ID: "col-1", Name: "id-1", Value: "id1"},
				{ID: "col-2", Name: "id-2", Value: float64(2.0)},
				{ID: "col-3", Name: "id-3", Value: int64(100)},
				{ID: "col-4", Name: "id-4", Value: int32(101)},
			},
			wantDoc: &Document{
				ID: fmt.Sprintf("%s_id1-2-100-101", testTable),
				Data: map[string]any{
					"col-1": "id1",
					"col-2": float64(2.0),
					"col-3": int64(100),
					"col-4": int32(101),
				},
			},
			wantErr: nil,
		},
		{
			name:      "error - no id columns",
			idColumns: []wal.Column{},
			wantDoc:   newDoc(""),
			wantErr:   processor.ErrIDNotFound,
		},
		{
			name: "error - nil",
			idColumns: []wal.Column{
				{Name: "id-1", Value: nil},
			},
			wantDoc: newDoc(""),
			wantErr: errNilIDValue,
		},
		{
			name: "error - unexpected type on single identity",
			idColumns: []wal.Column{
				{Name: "id-1", Value: true},
			},
			wantDoc: newDoc(""),
			wantErr: errUnsupportedType,
		},
		{
			name: "error - unexpected type on multiple identities",
			idColumns: []wal.Column{
				{ID: "col-1", Name: "id-1", Value: "id1"},
				{ID: "col-2", Name: "id-2", Value: true},
				{ID: "col-3", Name: "id-3", Value: int64(100)},
			},
			wantDoc: &Document{
				ID: "",
				Data: map[string]any{
					"col-1": "id1",
				},
			},
			wantErr: errUnsupportedType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &adapter{
				mapper: noopMapper,
			}
			doc := newDoc("")
			err := a.parseIDColumns(testTable, tc.idColumns, doc)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantDoc, doc)
		})
	}
}

func TestAdapter_IDHashing(t *testing.T) {
	t.Parallel()

	a := &adapter{idHasher: func(id string) string { return "hashed_" + id }}
	doc := &Document{Data: make(map[string]any)}
	err := a.parseIDColumns("tbl", []wal.Column{{Value: "myid"}}, doc)

	require.NoError(t, err)
	require.Equal(t, "tbl_hashed_myid", doc.ID)
}
