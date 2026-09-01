// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

// TestReplicaIdentityCheck_Run_Integration_CatalogQuery exercises
// replicaIdentityQuery against a real catalog.
//
// The two halves of this check are unit tested apart from the SQL:
// assessReplicaIdentity is a pure function over replicaIdentityRow, and
// TestReplicaIdentityCheck_Run_PluginScoping drives the scoping with a mocked
// querier. Both are handed rows that were written by hand, so neither can tell
// whether the query produces those rows for a real table.
//
// That matters most for replident_index_ok, which is not a column but a
// correlated NOT EXISTS over pg_attribute joined against pg_index's validity,
// uniqueness and partiality flags. Nothing else in the suite runs that clause.
//
// Worth recording, since it decides what this test can set up: postgres
// refuses ALTER TABLE ... REPLICA IDENTITY USING INDEX outright for a nullable
// or partial index, and refuses to drop NOT NULL from a column that is in one.
// The reachable way to end up with relreplident = 'i' and no usable index is to
// drop the index afterwards — postgres allows that and leaves relreplident at
// 'i', which is how a table silently stops replicating UPDATEs and DELETEs.
// That is the case covered below.
func TestReplicaIdentityCheck_Run_Integration_CatalogQuery(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	adminConn, err := pglib.NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer adminConn.Close(ctx)

	// one table per replica identity shape the query has to tell apart
	for _, ddl := range []string{
		// FULL: always sufficient
		`CREATE TABLE ri_full(id int, name text)`,
		`ALTER TABLE ri_full REPLICA IDENTITY FULL`,

		// default with a primary key: sufficient
		`CREATE TABLE ri_default_pk(id int PRIMARY KEY, name text)`,

		// default without a primary key: a finding
		`CREATE TABLE ri_default_no_pk(id int, name text)`,

		// NOTHING: a finding
		`CREATE TABLE ri_nothing(id int PRIMARY KEY, name text)`,
		`ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING`,

		// USING INDEX over a NOT NULL column: sufficient
		`CREATE TABLE ri_index_ok(id int NOT NULL, name text)`,
		`CREATE UNIQUE INDEX ri_index_ok_idx ON ri_index_ok(id)`,
		`ALTER TABLE ri_index_ok REPLICA IDENTITY USING INDEX ri_index_ok_idx`,

		// relreplident stays 'i' once the index it named is dropped, leaving
		// the table with an index identity and no index to serve it. This is
		// the only one of these shapes postgres will let us build, and the
		// only one where the LEFT JOIN matches nothing.
		`CREATE TABLE ri_index_dropped(id int NOT NULL, other int NOT NULL)`,
		`CREATE UNIQUE INDEX ri_index_dropped_idx ON ri_index_dropped(other)`,
		`ALTER TABLE ri_index_dropped REPLICA IDENTITY USING INDEX ri_index_dropped_idx`,
		`DROP INDEX ri_index_dropped_idx`,
	} {
		_, err = adminConn.Exec(ctx, ddl)
		require.NoError(t, err, ddl)
	}

	check := &ReplicaIdentityCheck{
		Source: func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConn(ctx, pgURL)
		},
		Selection: (&stream.Config{}).ReplicationTableSelection(),
	}

	findings, err := check.Run(ctx)
	require.NoError(t, err)

	joined := ""
	for _, f := range findings {
		joined += f.Message + "\n"
	}

	tests := []struct {
		table       string
		wantFinding bool
		wantReason  string
	}{
		{table: "ri_full", wantFinding: false},
		{table: "ri_default_pk", wantFinding: false},
		{table: "ri_index_ok", wantFinding: false},
		{
			table:       "ri_default_no_pk",
			wantFinding: true,
			wantReason:  "REPLICA IDENTITY=default but no PRIMARY KEY",
		},
		{
			table:       "ri_nothing",
			wantFinding: true,
			wantReason:  "REPLICA IDENTITY=nothing",
		},
		{
			table:       "ri_index_dropped",
			wantFinding: true,
			wantReason:  "REPLICA IDENTITY=index but the chosen index is invalid, non-unique, partial, or includes nullable columns",
		},
	}

	for _, tc := range tests {
		t.Run(tc.table, func(t *testing.T) {
			quoted := `"public"."` + tc.table + `"`
			if !tc.wantFinding {
				require.NotContains(t, joined, quoted,
					"%s has a sufficient replica identity but was flagged", tc.table)
				return
			}
			require.Contains(t, joined, quoted, "%s should have been flagged", tc.table)

			// the message has to name the reason, not just the table: a query
			// that mislabelled the identity would still flag the right table
			for _, f := range findings {
				if strings.HasPrefix(f.Message, quoted) {
					require.Contains(t, f.Message, tc.wantReason)
					return
				}
			}
			t.Fatalf("no finding started with %s", quoted)
		})
	}
}
