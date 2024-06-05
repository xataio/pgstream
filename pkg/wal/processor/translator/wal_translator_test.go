// SPDX-License-Identifier: Apache-2.0

package translator

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func TestTranslator_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testLogEntry := newTestLogEntry()

	tests := []struct {
		name       string
		event      *wal.Event
		store      schemalog.Store
		adapter    walToLogEntryAdapter
		skipSchema schemaFilter
		idFinder   columnFinder
		processor  processor.Processor

		wantErr error
	}{
		{
			name:       "ok - skip schema",
			event:      newTestDataEvent("I"),
			skipSchema: func(s string) bool { return true },

			wantErr: nil,
		},
		{
			name:       "ok - skip log entry schema log",
			event:      newTestSchemaChangeEvent("I"),
			skipSchema: func(s string) bool { return s == testSchemaName },

			wantErr: nil,
		},
		{
			name: "ok - schema event from ignored table",
			event: func() *wal.Event {
				d := newTestSchemaChangeEvent("I")
				d.Data.Table = "other"
				return d
			}(),

			wantErr: nil,
		},
		{
			name:  "ok - schema event from ignored action",
			event: newTestSchemaChangeEvent("U"),

			wantErr: nil,
		},
		{
			name:  "ok - schema event",
			event: newTestSchemaChangeEvent("I"),
			store: &schemalogmocks.Store{
				AckFn: func(ctx context.Context, le *schemalog.LogEntry) error {
					require.Equal(t, testLogEntry, le)
					return nil
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestSchemaChangeEvent("I"), walEvent)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - fail to ack schema event",
			event: newTestSchemaChangeEvent("I"),
			store: &schemalogmocks.Store{
				AckFn: func(ctx context.Context, le *schemalog.LogEntry) error {
					return errTest
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestSchemaChangeEvent("I"), walEvent)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - data event",
			event: newTestDataEvent("I"),
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return testLogEntry, nil
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestDataEventWithMetadata("I"), walEvent)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - fail to translate data event",
			event: newTestDataEvent("I"),
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestDataEvent("I"), walEvent)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - fail to translate data event with invalid data",
			event: newTestDataEvent("I"),
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					return testLogEntry, nil
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					wantEvent := newTestDataEvent("I")
					wantEvent.Data.Metadata = wal.Metadata{
						SchemaID:        testSchemaID,
						TablePgstreamID: testTableID,
					}
					require.Equal(t, wantEvent, walEvent)
					return nil
				},
			},
			idFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return false },

			wantErr: nil,
		},
		{
			name:    "error - adapting schema event",
			event:   newTestSchemaChangeEvent("I"),
			adapter: func(d *wal.Data) (*schemalog.LogEntry, error) { return nil, errTest },

			wantErr: errTest,
		},
		{
			name:  "error - processing event",
			event: newTestDataEvent("I"),
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					return testLogEntry, nil
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			translator := &Translator{
				processor:            tc.processor,
				schemaLogStore:       tc.store,
				skipSchema:           func(s string) bool { return false },
				idFinder:             func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },
				versionFinder:        func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-2" },
				walToLogEntryAdapter: func(d *wal.Data) (*schemalog.LogEntry, error) { return testLogEntry, nil },
			}

			if tc.idFinder != nil {
				translator.idFinder = tc.idFinder
			}

			if tc.adapter != nil {
				translator.walToLogEntryAdapter = tc.adapter
			}

			if tc.skipSchema != nil {
				translator.skipSchema = tc.skipSchema
			}

			err := translator.ProcessWALEvent(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestTranslator_translate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		data          *wal.Data
		store         schemalog.Store
		idFinder      columnFinder
		versionFinder columnFinder

		wantData *wal.Data
		wantErr  error
	}{
		{
			name: "ok - nil data",
			data: nil,

			wantData: nil,
			wantErr:  nil,
		},
		{
			name: "ok - default primary key finder",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data:          newTestDataEvent("I").Data,
			idFinder:      primaryKeyFinder,
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-2" },

			wantData: newTestDataEventWithMetadata("I").Data,
			wantErr:  nil,
		},
		{
			name: "ok - custom id finder",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data:          newTestDataEvent("I").Data,
			idFinder:      func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-2" },

			wantData: newTestDataEventWithMetadata("I").Data,
			wantErr:  nil,
		},
		{
			name: "ok - no version provided",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data:     newTestDataEvent("I").Data,
			idFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },

			wantData: func() *wal.Data {
				d := newTestDataEventWithMetadata("I").Data
				d.Metadata.InternalColVersion = ""
				return d
			}(),
			wantErr: nil,
		},
		{
			name: "error - fetching schema log entry",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},
			data: newTestDataEvent("I").Data,

			wantData: newTestDataEvent("I").Data,
			wantErr:  errTest,
		},
		{
			name: "error - schema not found",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					return nil, schemalog.ErrNoRows
				},
			},
			data: newTestDataEvent("I").Data,

			wantData: newTestDataEvent("I").Data,
			wantErr:  nil,
		},
		{
			name: "error - table not found",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Table = "unknown"
				return d
			}(),

			wantData: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Table = "unknown"
				return d
			}(),
			wantErr: processor.ErrTableNotFound,
		},
		{
			name: "error - filling metadata, id not found",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data:          newTestDataEvent("I").Data,
			idFinder:      func(c *schemalog.Column, _ *schemalog.Table) bool { return false },
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return false },

			wantData: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Metadata = wal.Metadata{
					SchemaID:        testSchemaID,
					TablePgstreamID: testTableID,
				}
				return d
			}(),
			wantErr: processor.ErrIDNotFound,
		},
		{
			name: "error - filling metadata, version not found",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data:          newTestDataEvent("I").Data,
			idFinder:      func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return false },

			wantData: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Metadata = wal.Metadata{
					SchemaID:        testSchemaID,
					TablePgstreamID: testTableID,
					InternalColID:   fmt.Sprintf("%s_col-1", testTableID),
				}
				return d
			}(),
			wantErr: processor.ErrVersionNotFound,
		},
		{
			name: "error - translating columns",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Columns = append(d.Columns, wal.Column{
					ID: "col-3", Name: "col-3", Type: "text", Value: "blob",
				})
				return d
			}(),
			idFinder:      func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-2" },

			wantData: func() *wal.Data {
				d := newTestDataEventWithMetadata("I").Data
				d.Columns = append(d.Columns, wal.Column{ID: "col-3", Name: "col-3", Type: "text", Value: "blob"})
				return d
			}(),
			wantErr: processor.ErrColumnNotFound,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			translator := &Translator{
				schemaLogStore: tc.store,
				idFinder:       tc.idFinder,
				versionFinder:  tc.versionFinder,
			}

			err := translator.translate(context.Background(), tc.data)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantData, tc.data)
		})
	}
}

func Test_primaryKeyFinder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		col  *schemalog.Column
		tbl  *schemalog.Table

		wantFound bool
	}{
		{
			name: "nil table",
			col: &schemalog.Column{
				Name: "col-1",
			},

			wantFound: false,
		},
		{
			name: "primary key",
			col: &schemalog.Column{
				Name: "col-1",
			},
			tbl: &schemalog.Table{
				PrimaryKeyColumns: []string{"col-1"},
			},

			wantFound: true,
		},
		{
			name: "no primary keys/unique not null columns",
			col: &schemalog.Column{
				Name: "col-1",
			},
			tbl: &schemalog.Table{},

			wantFound: false,
		},
		{
			name: "no primary keys with unique not null columns",
			col: &schemalog.Column{
				Name: "col-3",
			},
			tbl: &schemalog.Table{
				Columns: []schemalog.Column{
					{Name: "col-1", Unique: false, Nullable: true},
					{Name: "col-2", Unique: false, Nullable: true},
					{Name: "col-3", Unique: true, Nullable: false},
				},
			},

			wantFound: true,
		},
		{
			name: "composite primary key",
			col: &schemalog.Column{
				Name: "col-1",
			},
			tbl: &schemalog.Table{
				PrimaryKeyColumns: []string{"col-1", "col-2"},
			},

			wantFound: false,
		},
		{
			name: "composite primary key and unique not null column",
			col: &schemalog.Column{
				Name: "col-3",
			},
			tbl: &schemalog.Table{
				Columns: []schemalog.Column{
					{Name: "col-3", Unique: true, Nullable: false},
				},
				PrimaryKeyColumns: []string{"col-1", "col-2"},
			},

			wantFound: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			found := primaryKeyFinder(tc.col, tc.tbl)
			require.Equal(t, tc.wantFound, found)
		})
	}
}
