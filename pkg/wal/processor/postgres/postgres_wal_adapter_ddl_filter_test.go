// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// The predicate itself is pinned by TestDDLObjectTypeFilter_ShouldSkipDDL. What
// these two cover is the wiring on either side of it, which nothing else
// exercises: that newAdapter builds the filter out of the writer config, and
// that walEventToQueries actually consults it before emitting a DDL query.
// Between them, dropping either half — the newDDLObjectTypeFilter call in
// newAdapter, or the shouldSkipDDL call in walEventToQueries — replicates every
// filtered DDL statement to the target while the predicate's own tests stay
// green.

func TestNewAdapter_buildsTheDDLObjectTypeFilterFromConfig(t *testing.T) {
	t.Parallel()

	// the pool is lazy, so no server is contacted by constructing an adapter
	cfg := func(include, exclude []string) *Config {
		return &Config{
			URL:                   "postgres://user:pass@localhost:5432/testdb",
			RetryPolicy:           backoff.Config{DisableRetries: true},
			IncludeDDLObjectTypes: include,
			ExcludeDDLObjectTypes: exclude,
		}
	}

	createView := &wal.DDLEvent{
		CommandTag: "CREATE VIEW",
		Objects:    []wal.DDLObject{{Type: "view", Identity: "public.v", Schema: "public"}},
	}
	createTable := &wal.DDLEvent{
		CommandTag: "CREATE TABLE",
		Objects:    []wal.DDLObject{{Type: "table", Identity: "public.t", Schema: "public"}},
	}

	t.Run("include list reaches the filter", func(t *testing.T) {
		t.Parallel()

		a, err := newAdapter(context.Background(), loglib.NewNoopLogger(), cfg([]string{"tables", "sequences", "types"}, nil), false, 1)
		require.NoError(t, err)
		defer a.close()

		require.NotNil(t, a.ddlObjectTypeFilter, "the config declares a filter, so the adapter must hold one")
		require.True(t, a.ddlObjectTypeFilter.shouldSkipDDL(createView), "views are outside the include list")
		require.False(t, a.ddlObjectTypeFilter.shouldSkipDDL(createTable), "tables are inside the include list")
	})

	t.Run("exclude list reaches the filter", func(t *testing.T) {
		t.Parallel()

		a, err := newAdapter(context.Background(), loglib.NewNoopLogger(), cfg(nil, []string{"views"}), false, 1)
		require.NoError(t, err)
		defer a.close()

		require.NotNil(t, a.ddlObjectTypeFilter)
		require.True(t, a.ddlObjectTypeFilter.shouldSkipDDL(createView))
		require.False(t, a.ddlObjectTypeFilter.shouldSkipDDL(createTable))
	})

	t.Run("no filter configured leaves every DDL event through", func(t *testing.T) {
		t.Parallel()

		a, err := newAdapter(context.Background(), loglib.NewNoopLogger(), cfg(nil, nil), false, 1)
		require.NoError(t, err)
		defer a.close()

		require.Nil(t, a.ddlObjectTypeFilter)
		require.False(t, a.ddlObjectTypeFilter.shouldSkipDDL(createView))
	})

	t.Run("an invalid filter config fails construction", func(t *testing.T) {
		t.Parallel()

		_, err := newAdapter(context.Background(), loglib.NewNoopLogger(), cfg([]string{"tables"}, []string{"views"}), false, 1)
		require.Error(t, err, "include and exclude are mutually exclusive")
	})
}

func TestAdapter_walEventToQueries_consultsTheDDLObjectTypeFilter(t *testing.T) {
	t.Parallel()

	ddlQuery := &query{sql: "CREATE VIEW public.v AS SELECT 1", isDDL: true}

	tests := []struct {
		name     string
		include  []string
		ddlEvent *wal.DDLEvent

		wantExecuted bool
	}{
		{
			name:         "an excluded object type produces no query",
			include:      []string{"tables"},
			ddlEvent:     &wal.DDLEvent{CommandTag: "CREATE VIEW", Objects: []wal.DDLObject{{Type: "view"}}},
			wantExecuted: false,
		},
		{
			name:         "an included object type still produces its query",
			include:      []string{"tables"},
			ddlEvent:     &wal.DDLEvent{CommandTag: "CREATE TABLE", Objects: []wal.DDLObject{{Type: "table"}}},
			wantExecuted: true,
		},
		{
			name:         "with no filter configured nothing is skipped",
			include:      nil,
			ddlEvent:     &wal.DDLEvent{CommandTag: "CREATE VIEW", Objects: []wal.DDLObject{{Type: "view"}}},
			wantExecuted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filter, err := newDDLObjectTypeFilter(tc.include, nil)
			require.NoError(t, err)

			updated := false
			a := &adapter{
				ddlObjectTypeFilter: filter,
				ddlEventAdapter:     func(*wal.Data) (*wal.DDLEvent, error) { return tc.ddlEvent, nil },
				ddlAdapter: &mockDDLAdapter{
					walDataToQueriesFn: func(context.Context, *wal.Data) ([]*query, error) {
						return []*query{ddlQuery}, nil
					},
				},
				schemaObserver: &mockSchemaObserver{
					isMaterializedViewFn: func(schema, table string) bool { return false },
					updateFn:             func(*wal.DDLEvent) { updated = true },
				},
			}

			queries, err := a.walEventToQueries(context.Background(), &wal.Event{Data: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Schema: "public",
			}})
			require.NoError(t, err)

			// the schema observer is updated either way: its cache has to track
			// the source schema even for DDL the target will not replay
			require.True(t, updated, "the schema observer must see every DDL event")

			require.Len(t, queries, 1)
			if tc.wantExecuted {
				require.Equal(t, ddlQuery, queries[0])
				return
			}
			require.True(t, queries[0].IsEmpty(), "a filtered DDL event must not produce a query to run")
		})
	}
}
