// SPDX-License-Identifier: Apache-2.0

package injector

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func TestInjector_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testLogEntry := newTestLogEntry()

	tests := []struct {
		name            string
		event           *wal.Event
		store           schemalog.Store
		adapter         walToLogEntryAdapter
		skipDataEvent   dataEventFilter
		skipSchemaEvent schemaEventFilter
		idFinder        columnFinder
		processor       processor.Processor

		wantErr error
	}{
		{
			name:          "ok - skip schema",
			event:         newTestDataEvent("I"),
			skipDataEvent: func(*wal.Data) bool { return true },

			wantErr: nil,
		},
		{
			name:            "ok - skip log entry schema log",
			event:           newTestSchemaChangeEvent("I"),
			skipSchemaEvent: func(s *schemalog.LogEntry) bool { return s.SchemaName == testSchemaName },

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
			name:  "ok - fail to inject data event",
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
			name:  "ok - fail to inject data event with invalid data",
			event: newTestDataEvent("I"),
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					return testLogEntry, nil
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					wantEvent := newTestDataEvent("I")
					wantEvent.Data = nil
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

			injector := &Injector{
				logger:               loglib.NewNoopLogger(),
				processor:            tc.processor,
				schemaLogStore:       tc.store,
				skipDataEvent:        func(d *wal.Data) bool { return false },
				skipSchemaEvent:      func(*schemalog.LogEntry) bool { return false },
				idFinder:             func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },
				versionFinder:        func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return c.Name == "col-2", nil },
				walToLogEntryAdapter: func(d *wal.Data) (*schemalog.LogEntry, error) { return testLogEntry, nil },
			}

			if tc.idFinder != nil {
				injector.idFinder = tc.idFinder
			}

			if tc.adapter != nil {
				injector.walToLogEntryAdapter = tc.adapter
			}

			if tc.skipSchemaEvent != nil {
				injector.skipSchemaEvent = tc.skipSchemaEvent
			}

			if tc.skipDataEvent != nil {
				injector.skipDataEvent = tc.skipDataEvent
			}

			err := injector.ProcessWALEvent(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestInjector_inject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		data          *wal.Data
		store         schemalog.Store
		idFinder      columnFinder
		versionFinder columnFinderWithErr

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
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return c.Name == "col-2", nil },

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
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return c.Name == "col-2", nil },

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
			name: "ok - version finder provided with use LSN error",
			store: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchemaName, schemaName)
					return newTestLogEntry(), nil
				},
			},
			data:          newTestDataEvent("I").Data,
			idFinder:      func(c *schemalog.Column, _ *schemalog.Table) bool { return c.Name == "col-1" },
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return false, ErrUseLSN },

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
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return false, nil },

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
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return false, nil },

			wantData: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Metadata = wal.Metadata{
					SchemaID:        testSchemaID,
					TablePgstreamID: testTableID,
					InternalColIDs:  []string{fmt.Sprintf("%s_col-1", testTableID)},
				}
				return d
			}(),
			wantErr: processor.ErrVersionNotFound,
		},
		{
			name: "error - injecting column ids",
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
			versionFinder: func(c *schemalog.Column, _ *schemalog.Table) (bool, error) { return c.Name == "col-2", nil },

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

			injector := &Injector{
				logger:         loglib.NewNoopLogger(),
				schemaLogStore: tc.store,
				idFinder:       tc.idFinder,
				versionFinder:  tc.versionFinder,
			}

			err := injector.inject(context.Background(), tc.data)
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

			wantFound: true,
		},
		{
			name: "unique not null column with composite primary key",
			col: &schemalog.Column{
				Name: "col-3",
			},
			tbl: &schemalog.Table{
				Columns: []schemalog.Column{
					{Name: "col-3", Unique: true, Nullable: false},
				},
				PrimaryKeyColumns: []string{"col-1", "col-2"},
			},

			wantFound: false,
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
