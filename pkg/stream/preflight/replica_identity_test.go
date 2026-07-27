// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/stream"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func TestAssessReplicaIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		row      replicaIdentityRow
		wantHit  bool
		wantSubs []string // substrings that must appear in the finding
	}{
		{
			name: "FULL is always OK",
			row:  replicaIdentityRow{Schema: "public", Name: "x", Relreplident: "f"},
		},
		{
			name: "default + PK is OK",
			row:  replicaIdentityRow{Schema: "public", Name: "users", Relreplident: "d", HasPK: true},
		},
		{
			name:     "default without PK is a finding",
			row:      replicaIdentityRow{Schema: "public", Name: "audit_log", Relreplident: "d"},
			wantHit:  true,
			wantSubs: []string{`"public"."audit_log"`, "REPLICA IDENTITY=default", "no PRIMARY KEY"},
		},
		{
			name:     "nothing is a finding regardless of PK",
			row:      replicaIdentityRow{Schema: "public", Name: "events", Relreplident: "n", HasPK: true},
			wantHit:  true,
			wantSubs: []string{`"public"."events"`, "REPLICA IDENTITY=nothing"},
		},
		{
			name: "index with a valid index is OK",
			row:  replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "i", ReplidentOK: true},
		},
		{
			name:     "index with an invalid index is a finding",
			row:      replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "i"},
			wantHit:  true,
			wantSubs: []string{`"public"."t"`, "REPLICA IDENTITY=index"},
		},
		{
			name:     "unknown relreplident value is a finding",
			row:      replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "z"},
			wantHit:  true,
			wantSubs: []string{"unknown REPLICA IDENTITY"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := assessReplicaIdentity(tc.row)
			if !tc.wantHit {
				require.Empty(t, got)
				return
			}
			for _, sub := range tc.wantSubs {
				require.Contains(t, got, sub)
			}
		})
	}
}

// replicaIdentitySource returns an AcquireFunc whose Querier serves the given
// replica-identity rows from Query, matching the 5-column scan order of
// replicaIdentityQuery.
func replicaIdentitySource(t *testing.T, rows []replicaIdentityRow) postgres.AcquireFunc {
	t.Helper()
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return &mocks.Rows{
					NextFn: func(i uint) bool { return int(i) <= len(rows) },
					ScanFn: func(i uint, dest ...any) error {
						require.Len(t, dest, 5)
						row := rows[i-1]
						for idx, val := range []string{row.Schema, row.Name, row.Relreplident} {
							p, ok := dest[idx].(*string)
							require.True(t, ok)
							*p = val
						}
						for idx, val := range []bool{row.HasPK, row.ReplidentOK} {
							p, ok := dest[3+idx].(*bool)
							require.True(t, ok)
							*p = val
						}
						return nil
					},
					ErrFn: func() error { return nil },
				}, nil
			},
		}, nil
	}
}

// TestReplicaIdentityCheck_Run_PluginScoping proves the check honours wal2json
// plugin-level table scoping: a public table the plugin filters out is not
// flagged, while the same table is flagged when nothing scopes it out.
func TestReplicaIdentityCheck_Run_PluginScoping(t *testing.T) {
	t.Parallel()

	// public.typeorm_metadata has REPLICA IDENTITY default and no PRIMARY KEY —
	// a finding unless it's out of replication scope. labs.staff_member_onboarding
	// is in scope and has the same problem, so it must always be flagged.
	rows := []replicaIdentityRow{
		{Schema: "public", Name: "typeorm_metadata", Relreplident: "d", HasPK: false},
		{Schema: "labs", Name: "staff_member_onboarding", Relreplident: "d", HasPK: false},
	}

	tests := []struct {
		name           string
		cfg            *stream.Config
		wantFlagged    []string // "schema.name" substrings expected in findings
		wantNotFlagged []string
	}{
		{
			name: "filter_tables public.* skips public tables, still flags in-scope table",
			cfg: &stream.Config{Listener: stream.ListenerConfig{Postgres: &stream.PostgresListenerConfig{
				Replication: pgreplication.Config{
					PluginArguments: pgreplication.PluginArguments{FilterTables: "public.*"},
				},
			}}},
			wantFlagged:    []string{`"labs"."staff_member_onboarding"`},
			wantNotFlagged: []string{"typeorm_metadata"},
		},
		{
			name:           "nothing scopes it out - public table is flagged (true positive)",
			cfg:            &stream.Config{},
			wantFlagged:    []string{`"public"."typeorm_metadata"`, `"labs"."staff_member_onboarding"`},
			wantNotFlagged: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			check := &ReplicaIdentityCheck{
				Source:    replicaIdentitySource(t, rows),
				Selection: tc.cfg.ReplicationTableSelection(),
			}
			findings, err := check.Run(context.Background())
			require.NoError(t, err)

			joined := ""
			for _, f := range findings {
				joined += f.Message + "\n"
			}
			for _, sub := range tc.wantFlagged {
				require.Contains(t, joined, sub)
			}
			for _, sub := range tc.wantNotFlagged {
				require.NotContains(t, joined, sub)
			}
		})
	}
}
