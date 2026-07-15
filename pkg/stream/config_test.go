// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
)

func TestConfig_ReplicationTableSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		cfg            *Config
		wantInclude    []string
		wantExclude    []string
		wantInScope    [][2]string
		wantOutOfScope [][2]string
	}{
		{
			name: "no filter configured returns unfiltered selection",
			cfg:  &Config{},
		},
		{
			name: "empty filter returns unfiltered selection",
			cfg: &Config{
				Processor: ProcessorConfig{Filter: &filter.Config{}},
			},
		},
		{
			name: "include list propagates",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{IncludeTables: []string{"public.users", "public.orders"}},
				},
			},
			wantInclude: []string{"public.users", "public.orders"},
		},
		{
			name: "exclude list propagates",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{ExcludeTables: []string{"public.audit_log"}},
				},
			},
			wantExclude: []string{"public.audit_log"},
		},
		{
			name: "schema-only tables out of scope in exclude mode",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{
						ExcludeTables:    []string{"public.secrets"},
						SchemaOnlyTables: []string{"public.audit_log"},
					},
				},
			},
			wantExclude:    []string{"public.secrets"},
			wantInScope:    [][2]string{{"public", "users"}},
			wantOutOfScope: [][2]string{{"public", "secrets"}, {"public", "audit_log"}},
		},
		{
			name: "exact include entry beats schema-only wildcard",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{
						IncludeTables:    []string{"public.users"},
						SchemaOnlyTables: []string{"public.*"},
					},
				},
			},
			wantInclude:    []string{"public.users"},
			wantInScope:    [][2]string{{"public", "users"}},
			wantOutOfScope: [][2]string{{"public", "orders"}},
		},
		{
			name: "schema-only table beats wildcard include",
			cfg: &Config{
				Processor: ProcessorConfig{
					Filter: &filter.Config{
						IncludeTables:    []string{"public.*"},
						SchemaOnlyTables: []string{"public.audit_log"},
					},
				},
			},
			wantInclude:    []string{"public.*"},
			wantInScope:    [][2]string{{"public", "users"}},
			wantOutOfScope: [][2]string{{"public", "audit_log"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.cfg.ReplicationTableSelection()
			require.Equal(t, tc.wantInclude, got.Include())
			require.Equal(t, tc.wantExclude, got.Exclude())
			for _, st := range tc.wantInScope {
				require.True(t, got.IsTableInScope(st[0], st[1]), "expected %s.%s to be in scope", st[0], st[1])
			}
			for _, st := range tc.wantOutOfScope {
				require.False(t, got.IsTableInScope(st[0], st[1]), "expected %s.%s to be out of scope", st[0], st[1])
			}
		})
	}
}

func TestConfig_IsValid_ValidatesTableSelections(t *testing.T) {
	t.Parallel()

	baseListener := ListenerConfig{Postgres: &PostgresListenerConfig{URL: "postgres://source"}}
	baseProcessor := ProcessorConfig{Stdout: &StdoutProcessorConfig{}}

	t.Run("malformed snapshot adapter Tables surfaces from IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					URL: "postgres://source",
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{Tables: []string{"too.many.parts"}},
					},
				},
			},
			Processor: baseProcessor,
		}
		err := cfg.IsValid()
		require.Error(t, err)
		require.ErrorContains(t, err, "snapshot table selection")
	})

	t.Run("malformed filter IncludeTables surfaces from IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: baseListener,
			Processor: ProcessorConfig{
				Stdout: &StdoutProcessorConfig{},
				Filter: &filter.Config{IncludeTables: []string{"too.many.parts"}},
			},
		}
		err := cfg.IsValid()
		require.Error(t, err)
		require.ErrorContains(t, err, "replication table selection")
	})

	t.Run("malformed snapshot adapter SchemaOnlyTables surfaces from IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					URL: "postgres://source",
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{SchemaOnlyTables: []string{"too.many.parts"}},
					},
				},
			},
			Processor: baseProcessor,
		}
		err := cfg.IsValid()
		require.Error(t, err)
		require.ErrorContains(t, err, "snapshot schema-only table selection")
	})

	t.Run("malformed filter SchemaOnlyTables surfaces from IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: baseListener,
			Processor: ProcessorConfig{
				Stdout: &StdoutProcessorConfig{},
				Filter: &filter.Config{SchemaOnlyTables: []string{"too.many.parts"}},
			},
		}
		err := cfg.IsValid()
		require.Error(t, err)
		require.ErrorContains(t, err, "replication schema-only table selection")
	})

	t.Run("wildcard schema with specific table in snapshot SchemaOnlyTables surfaces from IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					URL: "postgres://source",
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{SchemaOnlyTables: []string{"*.audit_log"}},
						Schema:  &snapshotbuilder.SchemaSnapshotConfig{},
					},
				},
			},
			Processor: baseProcessor,
		}
		err := cfg.IsValid()
		require.Error(t, err)
		require.ErrorContains(t, err, "wildcard schema must be used with wildcard table")
	})

	t.Run("snapshot SchemaOnlyTables without schema snapshot surfaces from IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					URL: "postgres://source",
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{SchemaOnlyTables: []string{"public.audit_log"}},
						Data:    &pgsnapshotgenerator.Config{URL: "postgres://source"},
					},
				},
			},
			Processor: baseProcessor,
		}
		err := cfg.IsValid()
		require.Error(t, err)
		require.ErrorContains(t, err, "schema-only tables require a schema snapshot")
	})

	t.Run("well-formed selections pass IsValid", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					URL: "postgres://source",
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{Tables: []string{"public.users"}},
					},
				},
			},
			Processor: ProcessorConfig{
				Stdout: &StdoutProcessorConfig{},
				Filter: &filter.Config{IncludeTables: []string{"public.orders"}},
			},
		}
		require.NoError(t, cfg.IsValid())
	})
}

func TestNewTableSelection(t *testing.T) {
	t.Parallel()

	t.Run("valid include/exclude returns selection with cached maps", func(t *testing.T) {
		t.Parallel()
		sel, err := NewTableSelection([]string{"public.users"}, []string{"public.audit_log"})
		require.NoError(t, err)
		require.Equal(t, []string{"public.users"}, sel.Include())
		require.Equal(t, []string{"public.audit_log"}, sel.Exclude())

		require.True(t, sel.IsTableInScope("public", "users"))
		require.False(t, sel.IsTableInScope("public", "audit_log"))
		require.False(t, sel.IsTableInScope("public", "orders"))
	})

	t.Run("malformed include surfaces an error", func(t *testing.T) {
		t.Parallel()
		_, err := NewTableSelection([]string{"too.many.parts"}, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "include")
	})

	t.Run("malformed exclude surfaces an error", func(t *testing.T) {
		t.Parallel()
		_, err := NewTableSelection(nil, []string{"too.many.parts"})
		require.Error(t, err)
		require.ErrorContains(t, err, "exclude")
	})

	t.Run("empty inputs return an unfiltered selection", func(t *testing.T) {
		t.Parallel()
		sel, err := NewTableSelection(nil, nil)
		require.NoError(t, err)
		require.True(t, sel.IsUnfiltered())
		require.True(t, sel.IsTableInScope("anything", "goes"))
	})
}

func TestTableSelection_IsUnfiltered(t *testing.T) {
	t.Parallel()

	require.True(t, TableSelection{}.IsUnfiltered())

	includeOnly, err := NewTableSelection([]string{"public.x"}, nil)
	require.NoError(t, err)
	require.False(t, includeOnly.IsUnfiltered())

	excludeOnly, err := NewTableSelection(nil, []string{"public.x"})
	require.NoError(t, err)
	require.False(t, excludeOnly.IsUnfiltered())
}

func TestTableSelection_IsTableInScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		include []string
		exclude []string
		schema  string
		table   string
		want    bool
	}{
		{name: "unfiltered selection includes everything", schema: "public", table: "users", want: true},
		{name: "include match", include: []string{"public.users"}, schema: "public", table: "users", want: true},
		{name: "include miss", include: []string{"public.users"}, schema: "public", table: "orders", want: false},
		{name: "exclude match", exclude: []string{"public.audit_log"}, schema: "public", table: "audit_log", want: false},
		{name: "exclude miss", exclude: []string{"public.audit_log"}, schema: "public", table: "users", want: true},
		{name: "exclude wins over include when both match", include: []string{"public.users"}, exclude: []string{"public.users"}, schema: "public", table: "users", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sel, err := NewTableSelection(tc.include, tc.exclude)
			require.NoError(t, err)
			require.Equal(t, tc.want, sel.IsTableInScope(tc.schema, tc.table))
		})
	}
}

func TestConfig_SnapshotTableSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         *Config
		wantInclude []string
		wantExclude []string
	}{
		{
			name: "no snapshot configured returns unfiltered selection",
			cfg:  &Config{},
		},
		{
			name: "snapshot adapter include/exclude propagates",
			cfg: &Config{
				Listener: ListenerConfig{
					Postgres: &PostgresListenerConfig{
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Adapter: adapter.SnapshotConfig{
								Tables:         []string{"public.users"},
								ExcludedTables: []string{"public.audit_log"},
							},
						},
					},
				},
			},
			wantInclude: []string{"public.users"},
			wantExclude: []string{"public.audit_log"},
		},
		{
			name: "schema-only tables added to include list",
			cfg: &Config{
				Listener: ListenerConfig{
					Postgres: &PostgresListenerConfig{
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Adapter: adapter.SnapshotConfig{
								Tables:           []string{"public.users"},
								SchemaOnlyTables: []string{"public.audit_log"},
							},
						},
					},
				},
			},
			wantInclude: []string{"public.users", "public.audit_log"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.cfg.SnapshotTableSelection()
			require.Equal(t, tc.wantInclude, got.Include())
			require.Equal(t, tc.wantExclude, got.Exclude())
		})
	}
}

func TestConfig_AccessTableSelection(t *testing.T) {
	t.Parallel()

	snapInclude := func(tables ...string) *Config {
		return &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{Tables: tables},
					},
				},
			},
		}
	}
	snapExclude := func(tables ...string) *Config {
		return &Config{
			Listener: ListenerConfig{
				Postgres: &PostgresListenerConfig{
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{
						Adapter: adapter.SnapshotConfig{ExcludedTables: tables},
					},
				},
			},
		}
	}
	withRepInclude := func(c *Config, tables ...string) *Config {
		c.Processor.Filter = &filter.Config{IncludeTables: tables}
		return c
	}
	withRepExclude := func(c *Config, tables ...string) *Config {
		c.Processor.Filter = &filter.Config{ExcludeTables: tables}
		return c
	}

	tests := []struct {
		name        string
		cfg         *Config
		wantInclude []string
		wantExclude []string
	}{
		{
			name: "both unfiltered returns unfiltered",
			cfg:  &Config{},
		},
		{
			name: "only snapshot Include with no filter returns unfiltered (replication covers everything)",
			cfg:  snapInclude("public.users"),
		},
		{
			name: "only replication Include with no snapshot returns unfiltered (snapshot covers everything)",
			cfg:  withRepInclude(&Config{}, "public.users"),
		},
		{
			name:        "both Include — access Include is the union",
			cfg:         withRepInclude(snapInclude("public.orders", "public.users"), "public.users", "public.events"),
			wantInclude: []string{"public.orders", "public.users", "public.events"},
		},
		{
			name:        "both Exclude — access Exclude is the intersection",
			cfg:         withRepExclude(snapExclude("public.audit_log", "public.events"), "public.audit_log", "public.tmp"),
			wantExclude: []string{"public.audit_log"},
		},
		{
			name: "mixed Include + Exclude falls back to unfiltered",
			cfg:  withRepExclude(snapInclude("public.orders"), "public.audit_log"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.cfg.AccessTableSelection()
			require.ElementsMatch(t, tc.wantInclude, got.Include(), "Include lists differ")
			require.ElementsMatch(t, tc.wantExclude, got.Exclude(), "Exclude lists differ")
		})
	}
}

func TestConfig_ApplySnapshotRawJSONValues(t *testing.T) {
	t.Parallel()

	snapshotListenerCfg := func() *PostgresListenerConfig {
		return &PostgresListenerConfig{
			Snapshot: &snapshotbuilder.SnapshotListenerConfig{
				Data: &pgsnapshotgenerator.Config{},
			},
		}
	}

	tests := []struct {
		name string
		cfg  *Config
		want bool
	}{
		{
			name: "postgres target enables raw json values",
			cfg: &Config{
				Listener:  ListenerConfig{Postgres: snapshotListenerCfg()},
				Processor: ProcessorConfig{Postgres: &PostgresProcessorConfig{}},
			},
			want: true,
		},
		{
			name: "non postgres target keeps raw json values disabled",
			cfg: &Config{
				Listener:  ListenerConfig{Postgres: snapshotListenerCfg()},
				Processor: ProcessorConfig{Search: &SearchProcessorConfig{}},
			},
			want: false,
		},
		{
			name: "no data snapshot config is a no-op",
			cfg: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{
					Snapshot: &snapshotbuilder.SnapshotListenerConfig{},
				}},
				Processor: ProcessorConfig{Postgres: &PostgresProcessorConfig{}},
			},
			want: false,
		},
		{
			name: "no snapshot config is a no-op",
			cfg: &Config{
				Listener:  ListenerConfig{Postgres: &PostgresListenerConfig{}},
				Processor: ProcessorConfig{Postgres: &PostgresProcessorConfig{}},
			},
			want: false,
		},
		{
			name: "no postgres listener is a no-op",
			cfg: &Config{
				Processor: ProcessorConfig{Postgres: &PostgresProcessorConfig{}},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.cfg.applySnapshotRawJSONValues()

			got := false
			if tc.cfg.Listener.Postgres != nil &&
				tc.cfg.Listener.Postgres.Snapshot != nil &&
				tc.cfg.Listener.Postgres.Snapshot.Data != nil {
				got = tc.cfg.Listener.Postgres.Snapshot.Data.RawJSONValues
			}
			require.Equal(t, tc.want, got)
		})
	}
}
