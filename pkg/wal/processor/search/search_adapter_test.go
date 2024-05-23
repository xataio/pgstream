// SPDX-License-Identifier: Apache-2.0

package search

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	searchmocks "github.com/xataio/pgstream/pkg/wal/processor/search/mocks"
)

func TestAdapter_walDataToLogEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	nowStr := now.Format("2006-01-02 15:04:05")
	id := xid.New()

	testWalData := &wal.Data{
		Action: "I",
		Schema: schemalog.SchemaName,
		Table:  schemalog.TableName,
		Columns: []wal.Column{
			{ID: "id", Name: "id", Type: "text", Value: id.String()},
			{ID: "version", Name: "version", Type: "integer", Value: 0},
			{ID: "schema_name", Name: "schema_name", Type: "text", Value: "test_schema_1"},
			{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
		},
	}

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
				SchemaName: "test_schema_1",
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
			wantErr:      errInvalidData,
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

			a := newAdapter(&searchmocks.Mapper{})
			if tc.marshaler != nil {
				a.marshaler = tc.marshaler
			}
			if tc.unmarshaler != nil {
				a.unmarshaler = tc.unmarshaler
			}

			logEntry, err := a.walDataToLogEntry(tc.data)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLogEntry, logEntry)
		})
	}
}

func TestAdapter_walDataToDocument(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testWalData := func(action string) *wal.Data {
		data := &wal.Data{
			Action: action,
			Schema: testSchema,
			Table:  "test_table",
			Metadata: wal.Metadata{
				TablePgstreamID:    "table-1",
				InternalColID:      "col-1",
				InternalColVersion: "col-2",
			},
		}

		cols := []wal.Column{
			{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
			{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
			{ID: "col-3", Name: "name", Type: "text", Value: "a"},
		}

		switch action {
		case "D":
			data.Identity = cols
		default:
			data.Columns = cols
		}

		return data
	}

	errTest := errors.New("oh noes")

	noopMapper := &searchmocks.Mapper{
		MapColumnValueFn: func(column schemalog.Column, value any) (any, error) { return value, nil },
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
			data:   testWalData("I"),

			wantDoc: &Document{
				ID:      "table-1_id-1",
				Version: 0,
				Schema:  testSchema,
				Data: map[string]any{
					"col-3":  "a",
					"_table": "table-1",
				},
			},
			wantErr: nil,
		},
		{
			name:   "ok - insert event with identity columns",
			mapper: noopMapper,
			data: func() *wal.Data {
				d := testWalData("I")
				d.Identity = []wal.Column{
					{ID: "col-1", Name: "id", Type: "text", Value: "id-1"},
					{ID: "col-2", Name: "version", Type: "integer", Value: int64(0)},
					{ID: "col-4", Name: "toast", Type: "text", Value: "very-long-value"},
				}
				return d
			}(),
			wantDoc: &Document{
				ID:      "table-1_id-1",
				Version: 0,
				Schema:  testSchema,
				Data: map[string]any{
					"col-3":  "a",
					"col-4":  "very-long-value",
					"_table": "table-1",
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - insert event with identity columns and invalid type",
			data: func() *wal.Data {
				d := testWalData("I")
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
				ID:      "table-1_id-1",
				Version: 0,
				Schema:  testSchema,
				Data: map[string]any{
					"col-3":  "a",
					"_table": "table-1",
				},
			},
			wantErr: nil,
		},
		{
			name:   "ok - delete event",
			mapper: noopMapper,
			data:   testWalData("D"),

			wantDoc: &Document{
				ID:      "table-1_id-1",
				Version: 1,
				Schema:  testSchema,
				Data: map[string]any{
					"col-3":  "a",
					"_table": "table-1",
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
					TablePgstreamID:    "table-1",
					InternalColID:      "col-1",
					InternalColVersion: "col-2",
				},
			},

			wantDoc: nil,
			wantErr: errIDNotFound,
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
					TablePgstreamID:    "table-1",
					InternalColID:      "col-1",
					InternalColVersion: "col-2",
				},
			},

			wantDoc: nil,
			wantErr: errIDNotFound,
		},
		{
			name: "error - insert event with identity columns",
			data: func() *wal.Data {
				d := testWalData("I")
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

			a := newAdapter(tc.mapper)
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
		TablePgstreamID:    "table-1",
		InternalColID:      "col-1",
		InternalColVersion: "col-2",
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

		wantDoc *Document
		wantErr error
	}{
		{
			name:     "ok",
			columns:  testColumns,
			metadata: testMetadata,
			mapper:   noopMapper,

			wantDoc: &Document{
				ID:      "table-1_id-1",
				Version: 0,
				Data: map[string]any{
					"col-3":  "a",
					"_table": "table-1",
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
				ID:      "table-1_id-1",
				Version: 0,
				Data: map[string]any{
					"_table": "table-1",
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
			wantErr: errIDNotFound,
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
			wantErr: errVersionNotFound,
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

			a := newAdapter(tc.mapper)
			doc, err := a.parseColumns(tc.columns, tc.metadata)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantDoc, doc)
		})
	}
}

func TestAdapter_parseIDColumn(t *testing.T) {
	t.Parallel()

	testTable := "test-table"
	tests := []struct {
		name string
		id   any

		wantID  string
		wantErr error
	}{
		{
			name:    "ok - string",
			id:      "id-1",
			wantID:  "test-table_id-1",
			wantErr: nil,
		},
		{
			name:    "ok - int64",
			id:      int64(1),
			wantID:  "test-table_1",
			wantErr: nil,
		},
		{
			name:    "ok - float64",
			id:      float64(1.0),
			wantID:  "test-table_1",
			wantErr: nil,
		},
		{
			name:    "error - nil",
			id:      nil,
			wantID:  "",
			wantErr: errNilIDValue,
		},
		{
			name:    "error - unexpected type",
			id:      true,
			wantID:  "",
			wantErr: errUnsupportedType,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &adapter{}
			id, err := a.parseIDColumn(testTable, tc.id)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantID, id)
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
