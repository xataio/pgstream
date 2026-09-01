// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
)

func TestWAL2JSONCheck_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// probeErr is returned by pg_create_logical_replication_slot.
		probeErr error

		wantHit     bool
		wantErr     bool
		wantSubs    []string
		wantDropped bool
	}{
		{
			name:        "plugin present - slot created, then dropped, no finding",
			probeErr:    nil,
			wantDropped: true,
		},
		{
			// 58P01 undefined_file is left untyped by MapError, so it arrives as a
			// raw pgconn.PgError and is matched by SQLSTATE.
			name:     "plugin missing - 58P01 undefined_file",
			probeErr: &pgconn.PgError{Code: "58P01", Message: `could not access file "wal2json": No such file or directory`},
			wantHit:  true,
			wantSubs: []string{"wal2json output plugin not available", "install the wal2json package"},
		},
		{
			// message-only fallback when the code isn't surfaced.
			name:     "plugin missing - message fallback (no SQLSTATE)",
			probeErr: errors.New(`ERROR: could not access file "wal2json": No such file or directory`),
			wantHit:  true,
			wantSubs: []string{"wal2json output plugin not available"},
		},
		{
			// postgres 17.11+ consults the output_plugin_libraries allowlist
			// before looking for the library, and raises 42501 — the same code
			// as a role lacking REPLICATION — so this must be classified by
			// message or the check goes silent on every modern server without
			// wal2json. MapError types 42501 as ErrPermissionDenied, which is
			// what the probe actually receives.
			name: "plugin not in output_plugin_libraries - a wal2json finding, not a permission precondition",
			probeErr: &postgres.ErrPermissionDenied{
				Details: `library "wal2json" may not be used as an output plugin`,
			},
			wantHit:  true,
			wantSubs: []string{"wal2json output plugin not available", "output_plugin_libraries"},
		},
		{
			// the same rejection arriving untyped, as a raw pgconn error
			name:     "plugin not in output_plugin_libraries - raw 42501",
			probeErr: &pgconn.PgError{Code: "42501", Message: `library "wal2json" may not be used as an output plugin`},
			wantHit:  true,
			wantSubs: []string{"wal2json output plugin not available", "output_plugin_libraries"},
		},
		{
			// 55000 is mapped to ErrPreconditionFailed by MapError.
			name:     "wal_level not logical - inconclusive, not a wal2json finding",
			probeErr: &postgres.ErrPreconditionFailed{Details: `logical decoding requires "wal_level" >= "logical"`},
			// wal_level check owns this: neither a finding nor an error here.
		},
		{
			// 42501 is mapped to ErrPermissionDenied by MapError.
			name:     "role lacks REPLICATION - inconclusive, not a wal2json finding",
			probeErr: &postgres.ErrPermissionDenied{Details: "must be superuser or replication role to use replication slots"},
			// replication_role_attr check owns this.
		},
		{
			// 53400 configuration_limit_exceeded is left untyped by MapError.
			name:     "no free slots - inconclusive, headroom check owns it",
			probeErr: &pgconn.PgError{Code: "53400", Message: "all replication slots are in use"},
		},
		{
			name:     "unexpected error - check could not run",
			probeErr: errors.New("connection reset by peer"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var dropped bool
			conn := &mocks.Querier{
				QueryRowFn: func(_ context.Context, dest []any, query string, args ...any) error {
					require.Contains(t, query, "pg_create_logical_replication_slot")
					require.Contains(t, query, "wal2json")
					// slot name must be a valid, temporary probe slot name
					require.Len(t, args, 1)
					slotName, ok := args[0].(string)
					require.True(t, ok)
					require.True(t, strings.HasPrefix(slotName, "pgstream_wal2json_probe_"))
					require.NoError(t, postgres.IsValidReplicationSlotName(slotName))
					if tc.probeErr != nil {
						return tc.probeErr
					}
					b, ok := dest[0].(*bool)
					require.True(t, ok)
					*b = true
					return nil
				},
				ExecFn: func(_ context.Context, _ uint, query string, args ...any) (postgres.CommandTag, error) {
					require.Contains(t, query, "pg_drop_replication_slot")
					require.Len(t, args, 1)
					dropped = true
					return postgres.CommandTag{}, nil
				},
			}

			check := &WAL2JSONCheck{Source: func(context.Context) (postgres.Querier, error) {
				return conn, nil
			}}

			findings, err := check.Run(context.Background())

			if tc.wantErr {
				require.Error(t, err)
				require.Empty(t, findings)
				return
			}
			require.NoError(t, err)
			if tc.wantHit {
				require.Len(t, findings, 1)
				for _, sub := range tc.wantSubs {
					require.Contains(t, findings[0].Message, sub)
				}
			} else {
				require.Empty(t, findings)
			}
			require.Equal(t, tc.wantDropped, dropped)
		})
	}
}

func TestWAL2JSONProbeSlotName(t *testing.T) {
	t.Parallel()

	name1, err := wal2jsonProbeSlotName()
	require.NoError(t, err)
	name2, err := wal2jsonProbeSlotName()
	require.NoError(t, err)

	require.NotEqual(t, name1, name2, "probe slot names must be collision-resistant")
	require.True(t, strings.HasPrefix(name1, "pgstream_wal2json_probe_"))
	require.NoError(t, postgres.IsValidReplicationSlotName(name1))
	require.NoError(t, postgres.IsValidReplicationSlotName(name2))
}
