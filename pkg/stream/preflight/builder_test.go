// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestore "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	pgprocessor "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

const (
	testSourceURL = "postgres://user:pass@localhost:5432/mydb"

	// hook tests dial these, so they name hosts that resolve nowhere: the hook
	// under test answers the lookup itself and refuses the dial.
	testHookSourceURL = "postgres://user:pass@source.invalid:5432/mydb"
	testHookTargetURL = "postgres://user:pass@target.invalid:5432/targetdb"
)

func checkNames(checks []Check) []string {
	names := make([]string, 0, len(checks))
	for _, c := range checks {
		names = append(names, c.Name())
	}
	return names
}

func TestBuildSourceChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		opts      []SourceOption
		wantNames []string
	}{
		{
			name: "every source check by default",
			wantNames: []string{
				"source connectivity",
				"source_snapshot_single_instance",
				"wal_level",
				"wal2json",
				"replication_slot_headroom",
				"replication_role_attr",
				"replica_identity",
				"source_table_select_privileges",
				"source_sequence_select_privileges",
				"postgres_version",
				"schema_type_compatibility",
				"database_size",
				"snapshot_connection_headroom",
			},
		},
		{
			name: "resources category",
			opts: []SourceOption{WithSourceCategories(CategoryResources)},
			wantNames: []string{
				"database_size",
				"snapshot_connection_headroom",
			},
		},
		{
			name: "single category",
			opts: []SourceOption{WithSourceCategories(CategoryReplication)},
			wantNames: []string{
				"wal_level",
				"wal2json",
				"replication_slot_headroom",
				"replication_role_attr",
				"replica_identity",
			},
		},
		{
			name: "categories in registration order regardless of argument order",
			opts: []SourceOption{WithSourceCategories(CategorySchema, CategoryConnectivity)},
			wantNames: []string{
				"source connectivity",
				"source_snapshot_single_instance",
				"postgres_version",
				"schema_type_compatibility",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup, err := BuildSourceChecks(testSourceURL, tc.opts...)
			require.NoError(t, err)
			require.NotNil(t, cleanup)
			t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

			require.Equal(t, tc.wantNames, checkNames(checks))
		})
	}
}

func TestBuildSourceChecks_NoTargetChecks(t *testing.T) {
	t.Parallel()

	checks, cleanup, err := BuildSourceChecks(testSourceURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

	for _, c := range checks {
		switch tc := c.(type) {
		case *ConnectivityCheck:
			require.Equal(t, "source", tc.Label)
		case *PostgresVersionCheck:
			require.Nil(t, tc.Target, "source run must not compare against a target")
		case *SchemaExtensionCompatibilityCheck, *PostgresRangeTypeCheck,
			*TargetCreateDBPrivilegeCheck, *TargetCreateRolePrivilegeCheck:
			t.Fatalf("target check %q included in a source run", c.Name())
		}
	}
}

// TestBuildSourceChecks_EveryTableInScope pins the constructor's scoping
// contract: with no table selection configured, every check that inspects user
// tables covers the whole database.
func TestBuildSourceChecks_EveryTableInScope(t *testing.T) {
	t.Parallel()

	checks, cleanup, err := BuildSourceChecks(testSourceURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

	var scoped int
	for _, c := range checks {
		switch tc := c.(type) {
		case *SourceTableSelectPrivilegesCheck:
			require.True(t, tc.Selection.IsUnfiltered())
			scoped++
		case *SourceSequenceSelectPrivilegesCheck:
			require.True(t, tc.Selection.IsUnfiltered())
			scoped++
		case *SchemaTypeCompatibilityCheck:
			require.True(t, tc.Selection.IsUnfiltered())
			scoped++
		case *ReplicaIdentityCheck:
			require.True(t, tc.Selection.IsUnfiltered())
			scoped++
		}
	}
	require.Equal(t, 4, scoped, "every table-scoped check should be unfiltered")
}

func TestBuildSourceChecks_MissingURL(t *testing.T) {
	t.Parallel()

	checks, cleanup, err := BuildSourceChecks("")

	require.ErrorContains(t, err, "source postgres url is required")
	require.Empty(t, checks)
	require.NotNil(t, cleanup, "cleanup must be safe to defer on error")
	require.NoError(t, cleanup(context.Background()))
}

// errHookRefused is the sentinel the refusing dialler returns. A check that
// reports it went through the hook; a check that escaped the hook reaches the
// real resolver and fails with something else.
const errHookRefused = "dialling refused by test"

// countingRefusingConn makes every dial fail without touching the network: the
// lookup answers from memory and the dialler refuses. It counts both, so a
// connection that escaped the options is visible as a missing count as well as
// through the error it fails with.
type countingRefusingConn struct {
	mu      sync.Mutex
	lookups int
	dials   int
}

func (c *countingRefusingConn) options() []ConnOption {
	return []ConnOption{
		WithLookupFunc(func(context.Context, string) ([]string, error) {
			c.mu.Lock()
			c.lookups++
			c.mu.Unlock()
			return []string{"192.0.2.1"}, nil
		}),
		WithDialFunc(func(context.Context, string, string) (net.Conn, error) {
			c.mu.Lock()
			c.dials++
			c.mu.Unlock()
			return nil, errors.New(errHookRefused)
		}),
	}
}

func (c *countingRefusingConn) counts() (lookups, dials int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lookups, c.dials
}

// lookupsPerConnection measures how many times one connection resolves its
// host. The driver expands a URL into one connection target per fallback (the
// default sslmode=prefer produces one), and resolves each, so the count is a
// property of the URL rather than a number worth hard-coding.
func lookupsPerConnection(t *testing.T) int {
	t.Helper()

	c := &countingRefusingConn{}
	_, err := probeDialer(testHookSourceURL, c.options()...)(context.Background())
	require.ErrorContains(t, err, errHookRefused)

	lookups, _ := c.counts()
	require.Positive(t, lookups)
	return lookups
}

// requireRefusedByHook asserts every check failed through the dialler the
// options installed. A connection that escaped them would resolve the host
// itself and fail with a different error, so this, and not the bare "the check
// did not pass", is what proves the options decided which address was reached.
func requireRefusedByHook(t *testing.T, report Report) {
	t.Helper()

	require.NotEmpty(t, report.Results)
	for _, res := range report.Results {
		msg := ""
		if res.Err != nil {
			msg = res.Err.Error()
		}
		for _, f := range res.Findings {
			msg += " " + f.Message
		}
		require.Contains(t, msg, errHookRefused,
			"check %q did not fail through the supplied dialler", res.Name)
	}
}

// TestBuildSourceChecks_ConnOptions pins that the options reach every
// connection a full source run opens — each category's shared connection, the
// connectivity check's own connection and the exported snapshot probe's — and
// that the supplied dialler decides which address is reached, so every check
// reports a connection failure instead of connecting.
func TestBuildSourceChecks_ConnOptions(t *testing.T) {
	t.Parallel()

	// One per category shared connection (replication, access, schema,
	// resources), one for the connectivity check and one for the exported
	// snapshot probe's exporting connection. The dialler refuses that
	// exporting dial, so the probe never reaches its parallel probe
	// connections; those are covered by TestProbeDialer_ConnOptions.
	const wantConnectionsBeforeFirstRefusal = 6

	perConn := lookupsPerConnection(t)

	c := &countingRefusingConn{}
	checks, cleanup, err := BuildSourceChecks(testHookSourceURL, WithConnOptions(c.options()...))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

	report := Run(context.Background(), checks)
	require.Len(t, report.Results, len(checks))
	requireRefusedByHook(t, report)

	lookups, dials := c.counts()
	require.Equal(t, wantConnectionsBeforeFirstRefusal*perConn, lookups,
		"every connection must resolve through the supplied resolver")
	require.GreaterOrEqual(t, dials, wantConnectionsBeforeFirstRefusal,
		"the supplied dialler must be the one used")
}

// TestProbeDialer_ConnOptions pins the half of the coverage the source run
// cannot reach: the exported snapshot probe opens one connection per probe,
// and every one of them carries the options. The probe dials in parallel, so
// this also exercises the concurrency the options must tolerate.
func TestProbeDialer_ConnOptions(t *testing.T) {
	t.Parallel()

	const probes = 8

	perConn := lookupsPerConnection(t)

	c := &countingRefusingConn{}
	dial := probeDialer(testHookSourceURL, c.options()...)

	errs := make([]error, probes)
	var wg sync.WaitGroup
	for i := 0; i < probes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = dial(context.Background())
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.ErrorContains(t, err, errHookRefused)
	}

	lookups, dials := c.counts()
	require.Equal(t, probes*perConn, lookups, "every probe connection must carry the options")
	require.GreaterOrEqual(t, dials, probes)
}

// TestBuildSourceChecks_NoConnOptions pins that the connections are configured
// exactly as before when no option is supplied.
func TestBuildSourceChecks_NoConnOptions(t *testing.T) {
	t.Parallel()

	checks, cleanup, err := BuildSourceChecks(testHookSourceURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

	for _, c := range checks {
		if cc, ok := c.(*ConnectivityCheck); ok {
			require.Empty(t, cc.ConnOptions)
		}
	}
	require.Nil(t, postgresConnOptions(nil))
}

func TestPostgresConnOptions(t *testing.T) {
	t.Parallel()

	dial := func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("refused") }
	lookup := func(context.Context, string) ([]string, error) { return nil, errors.New("unresolved") }

	tests := []struct {
		name string
		opts []ConnOption
		want int
	}{
		{name: "none", opts: nil, want: 0},
		{name: "dial only", opts: []ConnOption{WithDialFunc(dial)}, want: 1},
		{name: "lookup only", opts: []ConnOption{WithLookupFunc(lookup)}, want: 1},
		{name: "both", opts: []ConnOption{WithDialFunc(dial), WithLookupFunc(lookup)}, want: 2},
		{name: "nil funcs", opts: []ConnOption{WithDialFunc(nil), WithLookupFunc(nil)}, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Len(t, postgresConnOptions(tc.opts), tc.want)
		})
	}
}

// TestBuildAccessChecks_ConnOptions pins that the options reach the target
// connection the access builder opens as well as the source one.
func TestBuildAccessChecks_ConnOptions(t *testing.T) {
	t.Parallel()

	perConn := lookupsPerConnection(t)
	c := &countingRefusingConn{}
	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: testHookSourceURL,
				Snapshot: &snapshotbuilder.SnapshotListenerConfig{
					Schema: &snapshotbuilder.SchemaSnapshotConfig{
						DumpRestore: &pgdumprestore.Config{
							TargetPGURL:    testHookTargetURL,
							CreateTargetDB: true,
						},
					},
				},
			},
		},
	}

	checks, cleanup := BuildAccessChecks(cfg, c.options()...)
	require.NotNil(t, cleanup)
	t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

	requireRefusedByHook(t, Run(context.Background(), checks))

	lookups, _ := c.counts()
	require.Equal(t, 2*perConn, lookups, "the source and target connections must both carry the options")
}

// TestBuildSchemaChecks_ConnOptions pins that the target connection the schema
// builder opens carries the options too. The checks only reach the target once
// the source answers, so the test acquires both connections directly.
func TestBuildSchemaChecks_ConnOptions(t *testing.T) {
	t.Parallel()

	perConn := lookupsPerConnection(t)
	c := &countingRefusingConn{}
	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{URL: testHookSourceURL},
		},
		Processor: stream.ProcessorConfig{
			Postgres: &stream.PostgresProcessorConfig{
				BatchWriter: pgprocessor.Config{URL: testHookTargetURL},
			},
		},
	}

	checks, cleanup := BuildSchemaChecks(cfg, c.options()...)
	require.NotNil(t, cleanup)
	t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

	versionCheck, ok := checks[0].(*PostgresVersionCheck)
	require.True(t, ok)
	require.NotNil(t, versionCheck.Target)

	_, err := versionCheck.Source(context.Background())
	require.Error(t, err)
	_, err = versionCheck.Target(context.Background())
	require.Error(t, err)

	lookups, _ := c.counts()
	require.Equal(t, 2*perConn, lookups)
}

// TestBuilders_OneEntryPerCategory pins the registry contract: a category is
// one Builders entry, and every entry is complete.
func TestBuilders_OneEntryPerCategory(t *testing.T) {
	t.Parallel()

	seen := map[Category]bool{}
	flags := map[string]bool{}
	for _, b := range Builders {
		require.NotEmpty(t, b.Category)
		require.NotEmpty(t, b.Flag)
		require.NotNil(t, b.Build)
		require.False(t, seen[b.Category], "duplicate category %q", b.Category)
		require.False(t, flags[b.Flag], "duplicate flag %q", b.Flag)
		seen[b.Category] = true
		flags[b.Flag] = true
	}
}

// snapshotSizedChecks returns the two checks BuildSourceChecks sizes from the
// snapshot connection demand.
func snapshotSizedChecks(t *testing.T, checks []Check) (*SnapshotConnectionsCheck, *SourceSnapshotInstanceCheck) {
	t.Helper()

	var (
		headroom *SnapshotConnectionsCheck
		instance *SourceSnapshotInstanceCheck
	)
	for _, c := range checks {
		switch tc := c.(type) {
		case *SnapshotConnectionsCheck:
			headroom = tc
		case *SourceSnapshotInstanceCheck:
			instance = tc
		}
	}
	require.NotNil(t, headroom, "snapshot_connection_headroom must be built")
	require.NotNil(t, instance, "source_snapshot_single_instance must be built")
	return headroom, instance
}

func TestBuildSourceChecks_WithSnapshotData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		opts       []SourceOption
		wantDemand uint
		wantProbes int
	}{
		{
			name:       "omitted option keeps the generator defaults",
			wantDemand: 4,
			wantProbes: 4,
		},
		{
			name:       "empty config matches the omitted option",
			opts:       []SourceOption{WithSnapshotData(&pgsnapshotgenerator.Config{})},
			wantDemand: 4,
			wantProbes: 4,
		},
		{
			name: "supplied worker counts size the demand",
			opts: []SourceOption{WithSnapshotData(&pgsnapshotgenerator.Config{
				SnapshotWorkers: 2,
				TableWorkers:    4,
			})},
			wantDemand: 8,
			wantProbes: 8,
		},
		{
			name: "partial config applies the default for the unset worker count",
			opts: []SourceOption{WithSnapshotData(&pgsnapshotgenerator.Config{
				SnapshotWorkers: 3,
			})},
			wantDemand: 12,
			wantProbes: 12,
		},
		{
			name: "probe count stays capped above the demand",
			opts: []SourceOption{WithSnapshotData(&pgsnapshotgenerator.Config{
				SnapshotWorkers: 8,
				TableWorkers:    8,
			})},
			wantDemand: 64,
			wantProbes: 16,
		},
		{
			name: "last option wins",
			opts: []SourceOption{
				WithSnapshotData(&pgsnapshotgenerator.Config{SnapshotWorkers: 2, TableWorkers: 2}),
				WithSnapshotData(&pgsnapshotgenerator.Config{SnapshotWorkers: 3, TableWorkers: 2}),
			},
			wantDemand: 6,
			wantProbes: 6,
		},
		{
			name: "a configuration after a nil one is used",
			opts: []SourceOption{
				WithSnapshotData(nil),
				WithSnapshotData(&pgsnapshotgenerator.Config{SnapshotWorkers: 2, TableWorkers: 2}),
			},
			wantDemand: 4,
			wantProbes: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup, err := BuildSourceChecks(testSourceURL, tc.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

			headroom, instance := snapshotSizedChecks(t, checks)
			require.Equal(t, tc.wantDemand, headroom.Demand)
			require.Equal(t, tc.wantProbes, instance.Probes)
		})
	}
}

// TestBuildSourceChecks_SnapshotDataChangesHeadroomFinding runs the built
// headroom check against a source whose available connections sit between the
// default demand and the supplied one, so the finding depends on the option
// alone.
func TestBuildSourceChecks_SnapshotDataChangesHeadroomFinding(t *testing.T) {
	t.Parallel()

	// max_connections=20, superuser_reserved_connections=3, 7 in use leaves 10
	// available: above the default demand of 4, below the supplied 32.
	tests := []struct {
		name        string
		opts        []SourceOption
		wantFinding bool
		wantSubs    []string
	}{
		{
			name: "default demand fits the available connections",
		},
		{
			name: "supplied demand exceeds them",
			opts: []SourceOption{WithSnapshotData(&pgsnapshotgenerator.Config{
				SnapshotWorkers: 4,
				TableWorkers:    8,
			})},
			wantFinding: true,
			wantSubs:    []string{"32 concurrent connections", "only 10 available"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup, err := BuildSourceChecks(testSourceURL, tc.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cleanup(context.Background())) })

			headroom, _ := snapshotSizedChecks(t, checks)
			headroom.Source = sourceWithConnLimits(t, 20, 3, 7)

			findings, err := headroom.Run(context.Background())
			require.NoError(t, err)
			if !tc.wantFinding {
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

func TestBuildSourceChecks_NilSnapshotData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []SourceOption
	}{
		{
			name: "nil configuration",
			opts: []SourceOption{WithSnapshotData(nil)},
		},
		{
			name: "nil configuration after a valid one",
			opts: []SourceOption{
				WithSnapshotData(&pgsnapshotgenerator.Config{SnapshotWorkers: 2, TableWorkers: 2}),
				WithSnapshotData(nil),
			},
		},
		{
			name: "nil configuration with a category filter",
			opts: []SourceOption{WithSnapshotData(nil), WithSourceCategories(CategoryResources)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup, err := BuildSourceChecks(testSourceURL, tc.opts...)

			require.ErrorIs(t, err, ErrNilSnapshotData)
			require.Empty(t, checks, "a nil snapshot config must not silently drop the snapshot checks")
			require.NotNil(t, cleanup, "cleanup must be safe to defer on error")
			require.NoError(t, cleanup(context.Background()))
		})
	}
}
