// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
)

func TestSourceTableSelectPrivilegesCheck_Run_AllTablesHaveSelect(t *testing.T) {
	t.Parallel()

	check := &SourceTableSelectPrivilegesCheck{
		Source: sourceWithRows(t, []sourceTableSelectPrivilegeRow{
			{Role: "pgstream_user", Schema: "public", Table: "orders", HasSelect: true},
			{Role: "pgstream_user", Schema: "public", Table: "users", HasSelect: true},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestSourceTableSelectPrivilegesCheck_Run_MissingSelectReturnsFinding(t *testing.T) {
	t.Parallel()

	check := &SourceTableSelectPrivilegesCheck{
		Source: sourceWithRows(t, []sourceTableSelectPrivilegeRow{
			{Role: "pgstream_user", Schema: "public", Table: "orders", HasSelect: false},
			{Role: "pgstream_user", Schema: "public", Table: "users", HasSelect: true},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `source role "pgstream_user"`)
	require.Contains(t, findings[0].Message, `"public"."orders"`)
	require.Contains(t, findings[0].Message, "GRANT SELECT")
}

func TestSourceTableSelectPrivilegesCheck_Run_MultipleMissingSelect(t *testing.T) {
	t.Parallel()

	check := &SourceTableSelectPrivilegesCheck{
		Source: sourceWithRows(t, []sourceTableSelectPrivilegeRow{
			{Role: "pgstream_user", Schema: "billing", Table: "invoices", HasSelect: false},
			{Role: "pgstream_user", Schema: "public", Table: "orders", HasSelect: false},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 2)
	require.Contains(t, findings[0].Message, `"billing"."invoices"`)
	require.Contains(t, findings[1].Message, `"public"."orders"`)
}

func TestSourceTableSelectPrivilegesCheck_Run_SourceAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &SourceTableSelectPrivilegesCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, checkErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestSourceTableSelectPrivilegesCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &SourceTableSelectPrivilegesCheck{
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
	require.ErrorContains(t, err, "querying source table privileges")
}

func TestSourceTableSelectPrivilegesCheck_Run_ScanFails(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("scan failed")
	check := &SourceTableSelectPrivilegesCheck{
		Source: sourceWithMockRows(&mocks.Rows{
			NextFn: func(i uint) bool { return i == 1 },
			ScanFn: func(uint, ...any) error {
				return scanErr
			},
			ErrFn: func() error { return nil },
		}),
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, scanErr)
	require.ErrorContains(t, err, "scanning row")
}

func TestSourceTableSelectPrivilegesCheck_Run_RowsErr(t *testing.T) {
	t.Parallel()

	rowsErr := errors.New("rows failed")
	check := &SourceTableSelectPrivilegesCheck{
		Source: sourceWithMockRows(&mocks.Rows{
			NextFn: func(uint) bool { return false },
			ScanFn: func(uint, ...any) error {
				t.Fatal("Scan should not be called")
				return nil
			},
			ErrFn: func() error { return rowsErr },
		}),
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, rowsErr)
	require.ErrorContains(t, err, "iterating rows")
}

func TestSourceTableSelectPrivilegesCheck_Run_FiltersTablesByScope(t *testing.T) {
	t.Parallel()

	check := &SourceTableSelectPrivilegesCheck{
		Tables:         []string{"public.*"},
		ExcludedTables: []string{"public.audit_log"},
		Source: sourceWithRows(t, []sourceTableSelectPrivilegeRow{
			{Role: "pgstream_user", Schema: "billing", Table: "invoices", HasSelect: false},
			{Role: "pgstream_user", Schema: "public", Table: "audit_log", HasSelect: false},
			{Role: "pgstream_user", Schema: "public", Table: "orders", HasSelect: false},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `"public"."orders"`)
}

func TestSourceTableSelectPrivilegesCheck_Run_InvalidTableSelection(t *testing.T) {
	t.Parallel()

	check := &SourceTableSelectPrivilegesCheck{
		Tables: []string{"too.many.parts"},
		Source: func(context.Context) (postgres.Querier, error) {
			t.Fatal("Source should not be called when table selection is invalid")
			return nil, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorContains(t, err, "parsing table selection")
	require.ErrorIs(t, err, postgres.ErrInvalidTableName)
}

func TestSourceTableSelectPrivilegesCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "source_table_select_privileges", (&SourceTableSelectPrivilegesCheck{}).Name())
}

func TestSourceTableSelectPrivilegeMessage(t *testing.T) {
	t.Parallel()

	msg := sourceTableSelectPrivilegeMessage(sourceTableSelectPrivilegeRow{
		Role:   "pgstream_user",
		Schema: "public",
		Table:  "orders",
	})

	require.Contains(t, msg, `source role "pgstream_user"`)
	require.Contains(t, msg, `"public"."orders"`)
	require.Contains(t, msg, `GRANT SELECT ON TABLE "public"."orders" TO "pgstream_user"`)
}

func TestSourceTableSelectPrivilegeMessage_QuotesMixedCaseIdentifiers(t *testing.T) {
	t.Parallel()

	msg := sourceTableSelectPrivilegeMessage(sourceTableSelectPrivilegeRow{
		Role:   "Replicator",
		Schema: "Reporting",
		Table:  "DailyRollup",
	})

	// Without quoting, Postgres folds these to lowercase and the GRANT silently
	// targets the wrong identifiers (or errors). Quoting keeps the statement
	// executable on the user's actual objects.
	require.Contains(t, msg, `GRANT SELECT ON TABLE "Reporting"."DailyRollup" TO "Replicator"`)
}

func TestBuildAccessChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *stream.Config
		wantChecks int
		wantTables []string
		wantExcl   []string
	}{
		{
			name: "no source postgres url returns no checks",
			cfg:  &stream.Config{},
		},
		{
			name: "source postgres url returns select privilege check",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
			},
			wantChecks: 1,
		},
		{
			name: "snapshot table scope is passed to check",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{
						URL: "postgres://source",
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Adapter: adapter.SnapshotConfig{
								Tables:         []string{"public.orders"},
								ExcludedTables: []string{"public.audit_log"},
							},
						},
					},
				},
			},
			wantChecks: 1,
			wantTables: []string{"public.orders"},
			wantExcl:   []string{"public.audit_log"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup := BuildAccessChecks(tc.cfg)

			require.Len(t, checks, tc.wantChecks)
			if tc.wantChecks == 0 {
				require.Nil(t, cleanup)
				return
			}
			require.NotNil(t, cleanup)
			check, ok := checks[0].(*SourceTableSelectPrivilegesCheck)
			require.True(t, ok)
			require.Equal(t, tc.wantTables, check.Tables)
			require.Equal(t, tc.wantExcl, check.ExcludedTables)
		})
	}
}

func TestBuildChecks_SelectedAccessOnly(t *testing.T) {
	t.Parallel()

	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: "postgres://source",
			},
		},
	}

	checks, cleanup := BuildChecks(cfg, []Category{CategoryAccess})

	require.NotNil(t, cleanup)
	require.Len(t, checks, 1)
	require.Equal(t, "source_table_select_privileges", checks[0].Name())
}

func sourceWithRows(t *testing.T, rows []sourceTableSelectPrivilegeRow) postgres.AcquireFunc {
	t.Helper()
	return sourceWithMockRows(privilegeRows(t, rows))
}

func sourceWithMockRows(rows postgres.Rows) postgres.AcquireFunc {
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return rows, nil
			},
		}, nil
	}
}

func privilegeRows(t *testing.T, rows []sourceTableSelectPrivilegeRow) postgres.Rows {
	t.Helper()
	return &mocks.Rows{
		NextFn: func(i uint) bool {
			return int(i) <= len(rows)
		},
		ScanFn: func(i uint, dest ...any) error {
			require.Len(t, dest, 4)
			row := rows[i-1]
			role, ok := dest[0].(*string)
			require.True(t, ok)
			schema, ok := dest[1].(*string)
			require.True(t, ok)
			table, ok := dest[2].(*string)
			require.True(t, ok)
			hasSelect, ok := dest[3].(*bool)
			require.True(t, ok)
			*role = row.Role
			*schema = row.Schema
			*table = row.Table
			*hasSelect = row.HasSelect
			return nil
		},
		ErrFn: func() error { return nil },
	}
}
