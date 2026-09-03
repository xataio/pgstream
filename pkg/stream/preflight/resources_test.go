// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/stream"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
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
		name      string
		cfg       *stream.Config
		wantNames []string
		// wantDemand is asserted only when the headroom check is expected.
		wantDemand uint
	}{
		{
			name: "no source postgres url returns no checks",
			cfg:  &stream.Config{},
		},
		{
			name: "source without data snapshot reports size only",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
			},
			wantNames: []string{"database_size"},
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
			wantNames:  []string{"database_size", "snapshot_connection_headroom"},
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
			wantNames:  []string{"database_size", "snapshot_connection_headroom"},
			wantDemand: 15,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup := BuildResourcesChecks(tc.cfg)

			if len(tc.wantNames) == 0 {
				require.Empty(t, checks)
				require.Nil(t, cleanup)
				return
			}
			names := make([]string, 0, len(checks))
			for _, c := range checks {
				names = append(names, c.Name())
			}
			require.Equal(t, tc.wantNames, names)
			require.NotNil(t, cleanup)
			if tc.wantDemand == 0 {
				return
			}
			connCheck, ok := checks[1].(*SnapshotConnectionsCheck)
			require.True(t, ok)
			require.Equal(t, tc.wantDemand, connCheck.Demand)
		})
	}
}

func TestDatabaseSizeCheck_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sizeBytes int64
		wantSize  string
	}{
		{name: "empty database", sizeBytes: 0, wantSize: "0 bytes"},
		{name: "under the kilobyte step", sizeBytes: 512, wantSize: "512 bytes"},
		{name: "kilobytes", sizeBytes: 10 * 1024, wantSize: "10 kB"},
		{name: "megabytes", sizeBytes: 5 * 1024 * 1024, wantSize: "5120 kB"},
		{name: "gigabytes", sizeBytes: 42 * 1024 * 1024 * 1024, wantSize: "42 GB"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			check := &DatabaseSizeCheck{
				Source: func(context.Context) (postgres.Querier, error) {
					return &mocks.Querier{
						QueryRowFn: func(_ context.Context, dest []any, query string, _ ...any) error {
							require.Contains(t, query, "pg_database_size")
							sizeDest, ok := dest[0].(*int64)
							require.True(t, ok)
							*sizeDest = tc.sizeBytes
							return nil
						},
					}, nil
				},
			}

			findings, err := check.Run(context.Background())

			require.NoError(t, err)
			require.Empty(t, findings, "the size report is informational")
			// Details stays typed; Summary renders it
			require.Equal(t, map[string]any{"database_size_bytes": tc.sizeBytes}, check.Details())
			require.Equal(t, tc.wantSize, check.Summary())
		})
	}
}

func TestDatabaseSizeCheck_Run_ConnectFails(t *testing.T) {
	t.Parallel()

	connErr := errors.New("boom")
	check := &DatabaseSizeCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, connErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, connErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestDatabaseSizeCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &DatabaseSizeCheck{
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
	require.ErrorContains(t, err, "querying database size")
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
