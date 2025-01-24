// SPDX-License-Identifier: Apache-2.0

package search

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	searchmocks "github.com/xataio/pgstream/pkg/wal/processor/search/mocks"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationmocks "github.com/xataio/pgstream/pkg/wal/replication/mocks"
)

func TestAdapter_walEventToMsg(t *testing.T) {
	t.Parallel()

	id := xid.New()
	now := time.Now().UTC().Round(time.Second)
	testLogEntrySize := 104
	testDocBytes := []byte("test-doc")

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column schemalog.Column, value any) (any, error) { return value, nil },
	}

	testLSNParser := &replicationmocks.LSNParser{
		FromStringFn: func(s string) (replication.LSN, error) { return replication.LSN(7773397064), nil },
	}

	tests := []struct {
		name      string
		event     *wal.Event
		marshaler func(any) ([]byte, error)
		mapper    Mapper

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
			name:  "ok - schema log event with insert",
			event: newTestSchemaChangeEvent("I", id, now),

			wantMsg: &msg{
				schemaChange: newTestLogEntry(id, now),
				bytesSize:    testLogEntrySize,
			},
			wantErr: nil,
		},
		{
			name:  "ok - schema log event with update",
			event: newTestSchemaChangeEvent("U", id, now),

			wantMsg: nil,
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
				MapColumnValueFn: func(column schemalog.Column, value any) (any, error) { return nil, errTest },
			},

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name:      "ok - data event to log entry",
			event:     newTestSchemaChangeEvent("I", id, now),
			marshaler: func(a any) ([]byte, error) { return nil, errTest },

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name: "error - data event empty metadata",
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := newAdapter(noopMapper, testLSNParser)

			if tc.marshaler != nil {
				a.marshaler = tc.marshaler
			}
			if tc.mapper != nil {
				a.mapper = tc.mapper
			}

			item, err := a.walEventToMsg(tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantMsg, item)
		})
	}
}

func TestAdapter_walDataToLogEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	id := xid.New()
	testWalData := newTestSchemaChangeEvent("I", id, now).Data
	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		marshaler   func(any) ([]byte, error)
		unmarshaler func([]byte, any) error
		data        *wal.Data

		wantLogEntry *schemalog.LogEntry
		wantErr      error
	}{
		{
			name: "ok",
			data: testWalData,

			wantLogEntry: &schemalog.LogEntry{
				ID:         id,
				Version:    0,
				SchemaName: testSchemaName,
				CreatedAt:  schemalog.NewSchemaCreatedAtTimestamp(now),
			},
			wantErr: nil,
		},
		{
			name: "error - invalid data",
			data: &wal.Data{
				Schema: "test_schema",
				Table:  "test_table",
			},

			wantLogEntry: nil,
			wantErr:      processor.ErrIncompatibleWalData,
		},
		{
			name:      "error - marshaling",
			marshaler: func(a any) ([]byte, error) { return nil, errTest },
			data:      testWalData,

			wantLogEntry: nil,
			wantErr:      errTest,
		},
		{
			name:        "error - unmarshaling",
			unmarshaler: func(b []byte, a any) error { return errTest },
			data:        testWalData,

			wantLogEntry: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := newAdapter(&searchmocks.Mapper{}, &replicationmocks.LSNParser{})
			if tc.marshaler != nil {
				a.marshaler = tc.marshaler
			}
			if tc.unmarshaler != nil {
				a.unmarshaler = tc.unmarshaler
			}

			logEntry, _, err := a.walDataToLogEntry(tc.data)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLogEntry, logEntry)
		})
	}
}

func TestAdapter_walDataToDocument(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"

	errTest := errors.New("oh noes")

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column schemalog.Column, value any) (any, error) { return value, nil },
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
				Version: 0,
				Schema:  testSchema,
				Data: map[string]any{
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
				MapColumnValueFn: func(column schemalog.Column, value any) (any, error) {
					if column.Name == "toast" {
						return nil, ErrTypeInvalid{Input: "toast"}
					}
					return value, nil
				},
			},

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: 0,
				Schema:  testSchema,
				Data: map[string]any{
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
				Version: 1,
				Schema:  testSchema,
				Data: map[string]any{
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
					TablePgstreamID:    testTableID,
					InternalColIDs:     []string{"col-1"},
					InternalColVersion: "col-2",
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
					TablePgstreamID:    testTableID,
					InternalColIDs:     []string{"col-1"},
					InternalColVersion: "col-2",
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
				MapColumnValueFn: func(column schemalog.Column, value any) (any, error) {
					if column.Name == "toast" {
						return nil, errTest
					}
					return value, nil
				},
			},

			wantDoc: nil,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := newAdapter(tc.mapper, testLSNParser)
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
		TablePgstreamID:    testTableID,
		InternalColIDs:     []string{"col-1"},
		InternalColVersion: "col-2",
	}

	testLSNStr := "1/CF54A048"
	testLSN := 7773397064
	testLSNParser := &replicationmocks.LSNParser{
		FromStringFn: func(s string) (replication.LSN, error) { return replication.LSN(testLSN), nil },
	}

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column schemalog.Column, value any) (any, error) { return value, nil },
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
				Version: 0,
				Data: map[string]any{
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
				MapColumnValueFn: func(column schemalog.Column, value any) (any, error) {
					if column.Name == "name" {
						return nil, ErrTypeInvalid{}
					}
					return value, nil
				},
			},

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: 0,
				Data: map[string]any{
					"_table": testTableID,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - version not found, default to use lsn",
			columns: []wal.Column{
				{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
				{ID: "col-3", Name: "name", Type: "text", Value: "a"},
			},
			metadata: wal.Metadata{
				TablePgstreamID: testTableID,
				InternalColIDs:  []string{"col-1"},
			},
			mapper: noopMapper,

			wantDoc: &Document{
				ID:      fmt.Sprintf("%s_id-1", testTableID),
				Version: testLSN,
				Data: map[string]any{
					"col-3":  "a",
					"_table": testTableID,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - version not found, incompatible LSN value",
			columns: []wal.Column{
				{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
				{ID: "col-3", Name: "name", Type: "text", Value: "a"},
			},
			metadata: wal.Metadata{
				TablePgstreamID: testTableID,
				InternalColIDs:  []string{"col-1"},
			},
			mapper: noopMapper,
			parser: &replicationmocks.LSNParser{
				FromStringFn: func(s string) (replication.LSN, error) { return 0, errTest },
			},

			wantDoc: nil,
			wantErr: errIncompatibleLSN,
		},
		{
			name: "error - version not found",
			columns: []wal.Column{
				{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
				{ID: "col-3", Name: "name", Type: "text", Value: "a"},
			},
			metadata: testMetadata,
			mapper:   noopMapper,

			wantDoc: nil,
			wantErr: processor.ErrVersionNotFound,
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
			name: "error - invalid version value",
			columns: []wal.Column{
				{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
				{ID: "col-2", Name: "version", Type: "integer", Value: nil},
			},
			metadata: testMetadata,
			mapper:   noopMapper,

			wantDoc: nil,
			wantErr: errNilVersionValue,
		},
		{
			name:     "error - mapping column value",
			columns:  testColumns,
			metadata: testMetadata,
			mapper: &searchmocks.Mapper{
				MapColumnValueFn: func(column schemalog.Column, value any) (any, error) {
					if column.Name == "name" {
						return nil, errTest
					}
					return value, nil
				},
			},

			wantDoc: nil,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := newAdapter(tc.mapper, testLSNParser)
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
		MapColumnValueFn: func(column schemalog.Column, value any) (any, error) { return value, nil },
	}

	newDoc := func(id string) *Document {
		return &Document{
			ID:   id,
			Data: make(map[string]any),
		}
	}

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
		tc := tc
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

func TestAdapter_parseVersionColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version any

		wantVersion int
		wantErr     error
	}{
		{
			name:        "ok - int64",
			version:     int64(1),
			wantVersion: 1,
			wantErr:     nil,
		},
		{
			name:        "ok - float64",
			version:     float64(0.5),
			wantVersion: 1,
			wantErr:     nil,
		},
		{
			name:        "ok - negative float64",
			version:     float64(-0.5),
			wantVersion: -1,
			wantErr:     nil,
		},
		{
			name:        "error - nil",
			version:     nil,
			wantVersion: 0,
			wantErr:     errNilVersionValue,
		},
		{
			name:        "error - unexpected type",
			version:     true,
			wantVersion: 0,
			wantErr:     errUnsupportedType,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &adapter{}
			version, err := a.parseVersionColumn(tc.version)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantVersion, version)
		})
	}
}
