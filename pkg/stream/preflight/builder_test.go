// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const testSourceURL = "postgres://user:pass@localhost:5432/mydb"

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
