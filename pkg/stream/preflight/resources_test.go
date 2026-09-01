// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
)

// sourceWithConnLimits returns an AcquireFunc whose Querier answers the
// SnapshotConnectionsCheck query with the given max_connections,
// superuser_reserved_connections and in-use count.
func sourceWithConnLimits(t *testing.T, maxConns, reserved, used int) postgres.AcquireFunc {
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryRowFn: func(_ context.Context, dest []any, _ string, _ ...any) error {
				maxDest, ok := dest[0].(*int)
				require.True(t, ok)
				reservedDest, ok := dest[1].(*int)
				require.True(t, ok)
				usedDest, ok := dest[2].(*int)
				require.True(t, ok)
				*maxDest, *reservedDest, *usedDest = maxConns, reserved, used
				return nil
			},
		}, nil
	}
}

func TestSnapshotConnectionsCheck_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		demand   uint
		maxConns int
		reserved int
		used     int
		wantHit  bool
		wantSubs []string
	}{
		{
			name:     "ample headroom",
			demand:   16,
			maxConns: 100,
			reserved: 3,
			used:     10,
		},
		{
			name:     "exactly fits",
			demand:   87,
			maxConns: 100,
			reserved: 3,
			used:     10,
		},
		{
			name:     "one over available is a finding",
			demand:   88,
			maxConns: 100,
			reserved: 3,
			used:     10,
			wantHit:  true,
			wantSubs: []string{"88 concurrent connections", "87 available", "max_connections=100", "superuser_reserved_connections=3", "10 in use"},
		},
		{
			name:     "reserved connections eat into headroom",
			demand:   5,
			maxConns: 10,
			reserved: 3,
			used:     3,
			wantHit:  true,
			wantSubs: []string{"5 concurrent connections", "4 available"},
		},
		{
			name:     "already over budget clamps available to zero",
			demand:   1,
			maxConns: 10,
			reserved: 3,
			used:     20,
			wantHit:  true,
			wantSubs: []string{"only 0 available"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			check := &SnapshotConnectionsCheck{
				Source: sourceWithConnLimits(t, tc.maxConns, tc.reserved, tc.used),
				Demand: tc.demand,
			}

			findings, err := check.Run(context.Background())

			require.NoError(t, err)
			if !tc.wantHit {
				require.Empty(t, findings)
				return
			}
			require.Len(t, findings, 1)
			for _, sub := range tc.wantSubs {
				require.Contains(t, findings[0].Message, sub)
			}
		})
	}
}

func TestBuildResourcesChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *stream.Config
		wantChecks int
		wantDemand uint
		wantSizes  bool
	}{
		{
			name: "no source postgres url returns no checks",
			cfg:  &stream.Config{},
		},
		{
			name: "source without data snapshot or tables returns no checks",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
			},
		},
		{
			name: "data snapshot with defaults sizes 1x4",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{
						URL: "postgres://source",
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Data: &pgsnapshotgenerator.Config{},
						},
					},
				},
			},
			wantChecks: 1,
			wantDemand: 4,
		},
		{
			name: "explicit workers multiply",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{
						URL: "postgres://source",
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Data: &pgsnapshotgenerator.Config{SnapshotWorkers: 3, TableWorkers: 5},
						},
					},
				},
			},
			wantChecks: 1,
			wantDemand: 15,
		},
		{
			name: "configured snapshot tables add the size report",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{
						URL: "postgres://source",
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Data:    &pgsnapshotgenerator.Config{},
							Adapter: adapter.SnapshotConfig{Tables: []string{"public.users"}},
						},
					},
				},
				Processor: stream.ProcessorConfig{
					Filter: &filter.Config{IncludeTables: []string{"public.users"}},
				},
			},
			wantChecks: 2,
			wantDemand: 4,
			wantSizes:  true,
		},
		{
			name: "configured tables without a data snapshot report sizes alone",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{
						URL: "postgres://source",
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Adapter: adapter.SnapshotConfig{Tables: []string{"public.users"}},
						},
					},
				},
				Processor: stream.ProcessorConfig{
					Filter: &filter.Config{IncludeTables: []string{"public.users"}},
				},
			},
			wantChecks: 1,
			wantSizes:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup := BuildResourcesChecks(tc.cfg)

			require.Len(t, checks, tc.wantChecks)
			if tc.wantChecks == 0 {
				require.Nil(t, cleanup)
				return
			}
			require.NotNil(t, cleanup)

			var sizeCheck *TableSizesCheck
			for _, c := range checks {
				switch check := c.(type) {
				case *SnapshotConnectionsCheck:
					require.Equal(t, tc.wantDemand, check.Demand)
				case *TableSizesCheck:
					sizeCheck = check
				}
			}
			if !tc.wantSizes {
				require.Nil(t, sizeCheck)
				return
			}
			require.NotNil(t, sizeCheck)
			require.False(t, sizeCheck.Selection.IsUnfiltered())
		})
	}
}

// tableSizeRows drives a TableSizesCheck from the given catalog rows.
func tableSizeRows(t *testing.T, rows []tableSize) postgres.AcquireFunc {
	t.Helper()
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return &mocks.Rows{
					NextFn: func(i uint) bool { return int(i) <= len(rows) },
					ScanFn: func(i uint, dest ...any) error {
						require.Len(t, dest, 4)
						row := rows[i-1]
						schema, ok := dest[0].(*string)
						require.True(t, ok)
						table, ok := dest[1].(*string)
						require.True(t, ok)
						bytes, ok := dest[2].(*int64)
						require.True(t, ok)
						indexBytes, ok := dest[3].(*int64)
						require.True(t, ok)
						*schema, *table, *bytes, *indexBytes = row.schema, row.table, row.bytes, row.indexBytes
						return nil
					},
					ErrFn: func() error { return nil },
				}, nil
			},
		}, nil
	}
}

// catalogEntry returns the catalog row for a "schema.table" name.
func catalogEntry(t *testing.T, catalog []tableSize, name string) tableSize {
	t.Helper()
	for _, row := range catalog {
		if fmt.Sprintf("%s.%s", row.schema, row.table) == name {
			return row
		}
	}
	t.Fatalf("no catalog row for %q", name)
	return tableSize{}
}

func TestTableSizesCheck_Run(t *testing.T) {
	t.Parallel()

	catalog := []tableSize{
		{schema: "public", table: "events", bytes: 8 * 1024 * 1024, indexBytes: 4 * 1024 * 1024},
		{schema: "public", table: "users", bytes: 2 * 1024 * 1024, indexBytes: 1024 * 1024},
		{schema: "billing", table: "invoices", bytes: 1024, indexBytes: 512},
		{schema: "public", table: "audit_log", bytes: 512, indexBytes: 0},
	}

	tests := []struct {
		name           string
		include        []string
		exclude        []string
		wantTables     []string
		wantTotal      int64
		wantIndexTotal int64
	}{
		{
			name:           "include list keeps only the listed tables",
			include:        []string{"public.users", "billing.invoices"},
			wantTables:     []string{"public.users", "billing.invoices"},
			wantTotal:      2*1024*1024 + 1024,
			wantIndexTotal: 1024*1024 + 512,
		},
		{
			name:           "schema wildcard keeps the whole schema",
			include:        []string{"public.*"},
			wantTables:     []string{"public.events", "public.users", "public.audit_log"},
			wantTotal:      8*1024*1024 + 2*1024*1024 + 512,
			wantIndexTotal: 4*1024*1024 + 1024*1024,
		},
		{
			name:           "exclude list drops the listed tables",
			exclude:        []string{"public.audit_log", "public.events"},
			wantTables:     []string{"public.users", "billing.invoices"},
			wantTotal:      2*1024*1024 + 1024,
			wantIndexTotal: 1024*1024 + 512,
		},
		{
			name:       "nothing in scope reports an empty set",
			include:    []string{"public.missing"},
			wantTables: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			selection, err := stream.NewTableSelection(tc.include, tc.exclude)
			require.NoError(t, err)
			check := &TableSizesCheck{Source: tableSizeRows(t, catalog), Selection: selection}

			findings, err := check.Run(context.Background())

			require.NoError(t, err)
			require.Empty(t, findings, "the size report is informational")

			details := check.Details()
			require.Equal(t, tc.wantTotal, details["tables_size_bytes"])
			require.Equal(t, tc.wantIndexTotal, details["indexes_size_bytes"],
				"index bytes are totalled separately from table bytes")

			reported, ok := details["tables"].([]map[string]any)
			require.True(t, ok)
			names := make([]string, 0, len(reported))
			for _, r := range reported {
				names = append(names, fmt.Sprintf("%s.%s", r["schema"], r["table"]))
			}
			require.Equal(t, tc.wantTables, names, "order follows the query's largest-first sort")

			for i, r := range reported {
				source := catalogEntry(t, catalog, names[i])
				// raw byte counts only: display belongs to Summary/ExpandedSummary
				require.Equal(t, map[string]any{
					"schema":           source.schema,
					"table":            source.table,
					"size_bytes":       source.bytes,
					"index_size_bytes": source.indexBytes,
				}, r)
			}
		})
	}
}

func TestTableSizesCheck_Report(t *testing.T) {
	t.Parallel()

	catalog := []tableSize{
		{schema: "public", table: "users", bytes: 5005312, indexBytes: 1236992},
		{schema: "public", table: "events", bytes: 2736128, indexBytes: 1384448},
		{schema: "billing", table: "invoices", bytes: 8192, indexBytes: 0},
	}
	check := &TableSizesCheck{Source: tableSizeRows(t, catalog)}

	_, err := check.Run(context.Background())
	require.NoError(t, err)

	require.Equal(t, "3 tables · 7568 kB + 2560 kB indexes", check.Summary())
	require.Equal(t, []string{
		"public.users         4888 kB  indexes 1208 kB",
		"public.events        2672 kB  indexes 1352 kB",
		"billing.invoices  8192 bytes  indexes 0 bytes",
		"total                7568 kB  indexes 2560 kB",
	}, check.ExpandedSummary())
}

func TestTableSizesCheck_ReportEmpty(t *testing.T) {
	t.Parallel()

	check := &TableSizesCheck{Source: tableSizeRows(t, nil)}

	_, err := check.Run(context.Background())
	require.NoError(t, err)

	require.Equal(t, "0 tables · 0 bytes + 0 bytes indexes", check.Summary())
	require.Nil(t, check.ExpandedSummary())
}

func TestTableSizesCheck_Run_ConnectFails(t *testing.T) {
	t.Parallel()

	connErr := errors.New("boom")
	check := &TableSizesCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, connErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, connErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestTableSizesCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &TableSizesCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return nil, queryErr
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying table sizes")
}

func TestTableSizesCheck_Run_RowsFail(t *testing.T) {
	t.Parallel()

	rowsErr := errors.New("rows failed")
	check := &TableSizesCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return &mocks.Rows{
						NextFn: func(uint) bool { return false },
						ScanFn: func(uint, ...any) error { return nil },
						ErrFn:  func() error { return rowsErr },
					}, nil
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, rowsErr)
	require.ErrorContains(t, err, "iterating rows")
}

func TestSnapshotConnectionsCheck_Run_ConnectFails(t *testing.T) {
	t.Parallel()

	connErr := errors.New("boom")
	check := &SnapshotConnectionsCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, connErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, connErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestSnapshotConnectionsCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &SnapshotConnectionsCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryRowFn: func(context.Context, []any, string, ...any) error {
					return queryErr
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying connection limits")
}
