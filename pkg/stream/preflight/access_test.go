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
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
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

	sel, err := stream.NewTableSelection([]string{"public.*"}, []string{"public.audit_log"})
	require.NoError(t, err)

	check := &SourceTableSelectPrivilegesCheck{
		Selection: sel,
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

func TestSourceTableSelectPrivilegesCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "source_table_select_privileges", (&SourceTableSelectPrivilegesCheck{}).Name())
}

func TestSourceSequenceSelectPrivilegesCheck_Run_AllSequencesHaveSelect(t *testing.T) {
	t.Parallel()

	check := &SourceSequenceSelectPrivilegesCheck{
		Source: sourceWithSequenceRows(t, []sourceSequenceSelectPrivilegeRow{
			{Role: "pgstream_user", TableSchema: "public", Table: "orders", SequenceSchema: "public", Sequence: "orders_id_seq", HasSelect: true},
			{Role: "pgstream_user", TableSchema: "public", Table: "users", SequenceSchema: "public", Sequence: "users_id_seq", HasSelect: true},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestSourceSequenceSelectPrivilegesCheck_Run_MissingSelectReturnsFinding(t *testing.T) {
	t.Parallel()

	check := &SourceSequenceSelectPrivilegesCheck{
		Source: sourceWithSequenceRows(t, []sourceSequenceSelectPrivilegeRow{
			{Role: "pgstream_user", TableSchema: "public", Table: "orders", SequenceSchema: "public", Sequence: "orders_id_seq", HasSelect: false},
			{Role: "pgstream_user", TableSchema: "public", Table: "users", SequenceSchema: "public", Sequence: "users_id_seq", HasSelect: true},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `source role "pgstream_user"`)
	require.Contains(t, findings[0].Message, "public.orders_id_seq")
	require.Contains(t, findings[0].Message, "GRANT SELECT ON SEQUENCE")
}

func TestSourceSequenceSelectPrivilegesCheck_Run_MultipleMissingSelect(t *testing.T) {
	t.Parallel()

	check := &SourceSequenceSelectPrivilegesCheck{
		Source: sourceWithSequenceRows(t, []sourceSequenceSelectPrivilegeRow{
			{Role: "pgstream_user", TableSchema: "billing", Table: "invoices", SequenceSchema: "billing", Sequence: "invoices_id_seq", HasSelect: false},
			{Role: "pgstream_user", TableSchema: "public", Table: "orders", SequenceSchema: "public", Sequence: "orders_id_seq", HasSelect: false},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 2)
	require.Contains(t, findings[0].Message, "billing.invoices_id_seq")
	require.Contains(t, findings[1].Message, "public.orders_id_seq")
}

func TestSourceSequenceSelectPrivilegesCheck_Run_SourceAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &SourceSequenceSelectPrivilegesCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, checkErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestSourceSequenceSelectPrivilegesCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &SourceSequenceSelectPrivilegesCheck{
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
	require.ErrorContains(t, err, "querying source sequence privileges")
}

func TestSourceSequenceSelectPrivilegesCheck_Run_ScanFails(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("scan failed")
	check := &SourceSequenceSelectPrivilegesCheck{
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

func TestSourceSequenceSelectPrivilegesCheck_Run_RowsErr(t *testing.T) {
	t.Parallel()

	rowsErr := errors.New("rows failed")
	check := &SourceSequenceSelectPrivilegesCheck{
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

func TestSourceSequenceSelectPrivilegesCheck_Run_FiltersTablesByScope(t *testing.T) {
	t.Parallel()

	sel, err := stream.NewTableSelection([]string{"public.*"}, []string{"public.audit_log"})
	require.NoError(t, err)

	check := &SourceSequenceSelectPrivilegesCheck{
		Selection: sel,
		Source: sourceWithSequenceRows(t, []sourceSequenceSelectPrivilegeRow{
			{Role: "pgstream_user", TableSchema: "billing", Table: "invoices", SequenceSchema: "billing", Sequence: "invoices_id_seq", HasSelect: false},
			{Role: "pgstream_user", TableSchema: "public", Table: "audit_log", SequenceSchema: "public", Sequence: "audit_log_id_seq", HasSelect: false},
			{Role: "pgstream_user", TableSchema: "public", Table: "orders", SequenceSchema: "public", Sequence: "orders_id_seq", HasSelect: false},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, "public.orders_id_seq")
}

func TestSourceSequenceSelectPrivilegesCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "source_sequence_select_privileges", (&SourceSequenceSelectPrivilegesCheck{}).Name())
}

func TestSourceTableSelectPrivilegeMessage(t *testing.T) {
	t.Parallel()

	msg := sourceTableSelectPrivilegeMessage(sourceTableSelectPrivilegeRow{
		Role:   "pgstream_user",
		Schema: "public",
		Table:  "orders",
	})

	require.Contains(t, msg, `source role "pgstream_user"`)
	require.Contains(t, msg, "lacks SELECT on public.orders;")
	require.Contains(t, msg, `GRANT SELECT ON TABLE "public"."orders" TO "pgstream_user"`)
}

func TestSourceTableSelectPrivilegeMessage_QuotesOnlyRemediation(t *testing.T) {
	t.Parallel()

	// Descriptive prose stays human-readable (unquoted); the GRANT statement
	// gets postgres.QuoteIdentifier so it's executable on case-sensitive or
	// special-character names.
	msg := sourceTableSelectPrivilegeMessage(sourceTableSelectPrivilegeRow{
		Role:   "Replicator",
		Schema: "Reporting",
		Table:  "DailyRollup",
	})

	require.Contains(t, msg, "lacks SELECT on Reporting.DailyRollup;")
	require.Contains(t, msg, `GRANT SELECT ON TABLE "Reporting"."DailyRollup" TO "Replicator"`)
}

func TestSourceSequenceSelectPrivilegeMessage(t *testing.T) {
	t.Parallel()

	msg := sourceSequenceSelectPrivilegeMessage(sourceSequenceSelectPrivilegeRow{
		Role:           "pgstream_user",
		SequenceSchema: "public",
		Sequence:       "orders_id_seq",
	})

	require.Contains(t, msg, `source role "pgstream_user"`)
	require.Contains(t, msg, "lacks SELECT on sequence public.orders_id_seq;")
	require.Contains(t, msg, `GRANT SELECT ON SEQUENCE "public"."orders_id_seq" TO "pgstream_user"`)
}

func TestBuildAccessChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         *stream.Config
		wantChecks  int
		wantInclude []string
		wantExclude []string
	}{
		{
			name: "no source postgres url returns no checks",
			cfg:  &stream.Config{},
		},
		{
			name: "source postgres url returns access checks",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
			},
			wantChecks: 2,
		},
		{
			name: "snapshot+filter Include unions through AccessTableSelection",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{
						URL: "postgres://source",
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Adapter: adapter.SnapshotConfig{
								Tables: []string{"public.orders"},
							},
						},
					},
				},
				Processor: stream.ProcessorConfig{
					Filter: &filter.Config{
						IncludeTables: []string{"public.users"},
					},
				},
			},
			wantChecks:  2,
			wantInclude: []string{"public.orders", "public.users"},
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
			tableCheck, ok := checks[0].(*SourceTableSelectPrivilegesCheck)
			require.True(t, ok)
			require.ElementsMatch(t, tc.wantInclude, tableCheck.Selection.Include(), "Include lists differ")
			require.ElementsMatch(t, tc.wantExclude, tableCheck.Selection.Exclude(), "Exclude lists differ")
			sequenceCheck, ok := checks[1].(*SourceSequenceSelectPrivilegesCheck)
			require.True(t, ok)
			require.ElementsMatch(t, tc.wantInclude, sequenceCheck.Selection.Include(), "Include lists differ")
			require.ElementsMatch(t, tc.wantExclude, sequenceCheck.Selection.Exclude(), "Exclude lists differ")
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
	require.Len(t, checks, 2)
	require.Equal(t, "source_table_select_privileges", checks[0].Name())
	require.Equal(t, "source_sequence_select_privileges", checks[1].Name())
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

func sourceWithSequenceRows(t *testing.T, rows []sourceSequenceSelectPrivilegeRow) postgres.AcquireFunc {
	t.Helper()
	return sourceWithMockRows(sequencePrivilegeRows(t, rows))
}

func sequencePrivilegeRows(t *testing.T, rows []sourceSequenceSelectPrivilegeRow) postgres.Rows {
	t.Helper()
	return &mocks.Rows{
		NextFn: func(i uint) bool {
			return int(i) <= len(rows)
		},
		ScanFn: func(i uint, dest ...any) error {
			require.Len(t, dest, 6)
			row := rows[i-1]
			role, ok := dest[0].(*string)
			require.True(t, ok)
			tableSchema, ok := dest[1].(*string)
			require.True(t, ok)
			table, ok := dest[2].(*string)
			require.True(t, ok)
			sequenceSchema, ok := dest[3].(*string)
			require.True(t, ok)
			sequence, ok := dest[4].(*string)
			require.True(t, ok)
			hasSelect, ok := dest[5].(*bool)
			require.True(t, ok)
			*role = row.Role
			*tableSchema = row.TableSchema
			*table = row.Table
			*sequenceSchema = row.SequenceSchema
			*sequence = row.Sequence
			*hasSelect = row.HasSelect
			return nil
		},
		ErrFn: func() error { return nil },
	}
}
