// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// Test_BulkQueries_AcceptedByPostgres executes the SQL produced by the bulk
// coalescing builders against a real Postgres.
//
// The shape of every generated statement is pinned by the unit tests in
// postgres_wal_dml_adapter_bulk_test.go, which are cheaper and far more
// thorough. What needs a live engine is the other half: that Postgres accepts
// those statements and applies exactly the rows the builders intended — the
// array casts bind, the composite unnest projection matches on the whole key
// rather than one column of it, and the ON CONFLICT target resolves.
//
// This replaces three full-pipeline tests that reached the same builders
// through a replication slot and a polling loop.
func Test_BulkQueries_AcceptedByPostgres(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	conn, err := pglib.NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	tests := []struct {
		name string
		// setup runs before the generated queries; it creates the table and
		// seeds whatever state the case needs.
		setup []string
		// onConflictAction configures the adapter, empty for the default.
		onConflictAction string
		build            func(t *testing.T, a *dmlAdapter) []*query
		// wantSQL ties the statement actually executed here to the shape the
		// unit tests pin, so this test cannot silently drift onto another path.
		wantSQL string
		assert  func(t *testing.T, conn pglib.Querier)
	}{
		{
			// a run of 200 same-table inserts coalesces into a single
			// multi-row INSERT
			name: "bulk insert",
			setup: []string{
				`CREATE TABLE bulk_insert(id bigint PRIMARY KEY, name text, value bigint)`,
			},
			build: func(t *testing.T, a *dmlAdapter) []*query {
				events := make([]*wal.Data, 0, 200)
				for i := 1; i <= 200; i++ {
					events = append(events, &wal.Data{
						Action: "I",
						Schema: "public",
						Table:  "bulk_insert",
						Columns: []wal.Column{
							{Name: "id", Type: "bigint", Value: float64(i)},
							{Name: "name", Type: "text", Value: fmt.Sprintf("row_%d", i)},
							{Name: "value", Type: "bigint", Value: float64(i)},
						},
					})
				}
				queries := a.buildBulkInsertQueries(events, emptySchemaInfo())
				require.Len(t, queries, 1, "200 rows should coalesce into one insert")
				return queries
			},
			wantSQL: "OVERRIDING SYSTEM VALUE",
			assert: func(t *testing.T, conn pglib.Querier) {
				requireCount(t, conn, "SELECT count(*) FROM bulk_insert", 200)
				// spot check the ends of the value list, where an off-by-one in
				// the placeholder numbering would show up
				requireNameAt(t, conn, "SELECT name FROM bulk_insert WHERE id = 1", "row_1")
				requireNameAt(t, conn, "SELECT name FROM bulk_insert WHERE id = 200", "row_200")
			},
		},
		{
			// single-column key: DELETE ... WHERE id = ANY($1::int4[])
			name: "bulk delete on a single-column primary key",
			setup: []string{
				`CREATE TABLE bulk_delete_single(id integer PRIMARY KEY, name text)`,
				`INSERT INTO bulk_delete_single(id, name)
					SELECT g, 'row_' || g FROM generate_series(1, 200) g`,
			},
			build: func(t *testing.T, a *dmlAdapter) []*query {
				events := make([]*wal.Data, 0, 100)
				for i := 1; i <= 100; i++ {
					events = append(events, &wal.Data{
						Action: "D",
						Schema: "public",
						Table:  "bulk_delete_single",
						Identity: []wal.Column{
							{Name: "id", Type: "integer", Value: float64(i)},
						},
					})
				}
				queries, err := a.buildBulkDeleteQuery(events, emptySchemaInfo())
				require.NoError(t, err)
				require.Len(t, queries, 1, "100 deletes should coalesce into one query")
				return queries
			},
			wantSQL: "ANY($1::int4[])",
			assert: func(t *testing.T, conn pglib.Querier) {
				requireCount(t, conn, "SELECT count(*) FROM bulk_delete_single", 100)
				requireCount(t, conn, "SELECT count(*) FROM bulk_delete_single WHERE id <= 100", 0)
				requireCount(t, conn, "SELECT min(id) FROM bulk_delete_single", 101)
			},
		},
		{
			// composite key: the unnest projection has to match on the pair.
			// The decoy row shares item_id values with the deleted range but
			// belongs to another tenant: a projection that matched on one
			// column would take it too.
			name: "bulk delete on a composite primary key",
			setup: []string{
				`CREATE TABLE bulk_delete_composite(
					tenant_id bigint,
					item_id   bigint,
					name      text,
					PRIMARY KEY(tenant_id, item_id)
				)`,
				`INSERT INTO bulk_delete_composite(tenant_id, item_id, name)
					SELECT 1, g, 'item_' || g FROM generate_series(1, 50) g`,
				`INSERT INTO bulk_delete_composite(tenant_id, item_id, name)
					SELECT 2, g, 'other_tenant_' || g FROM generate_series(1, 25) g`,
			},
			build: func(t *testing.T, a *dmlAdapter) []*query {
				events := make([]*wal.Data, 0, 25)
				for i := 1; i <= 25; i++ {
					events = append(events, &wal.Data{
						Action: "D",
						Schema: "public",
						Table:  "bulk_delete_composite",
						Identity: []wal.Column{
							{Name: "tenant_id", Type: "bigint", Value: float64(1)},
							{Name: "item_id", Type: "bigint", Value: float64(i)},
						},
					})
				}
				queries, err := a.buildBulkDeleteQuery(events, emptySchemaInfo())
				require.NoError(t, err)
				require.Len(t, queries, 1)
				return queries
			},
			wantSQL: `("tenant_id","item_id") IN (SELECT * FROM unnest($1::int8[],$2::int8[]))`,
			assert: func(t *testing.T, conn pglib.Querier) {
				requireCount(t, conn,
					"SELECT count(*) FROM bulk_delete_composite WHERE tenant_id = 1", 25)
				requireCount(t, conn,
					"SELECT count(*) FROM bulk_delete_composite WHERE tenant_id = 2", 25)
			},
		},
		{
			// the conflicting row is seeded first, so the ON CONFLICT target
			// is actually exercised rather than merely present in the SQL
			name: "bulk insert with on conflict do nothing",
			setup: []string{
				`CREATE TABLE bulk_insert_conflict(id bigint PRIMARY KEY, name text)`,
				`INSERT INTO bulk_insert_conflict(id, name) VALUES (1, 'existing')`,
			},
			onConflictAction: "nothing",
			build: func(t *testing.T, a *dmlAdapter) []*query {
				events := make([]*wal.Data, 0, 50)
				for i := 1; i <= 50; i++ {
					events = append(events, &wal.Data{
						Action: "I",
						Schema: "public",
						Table:  "bulk_insert_conflict",
						Columns: []wal.Column{
							{Name: "id", Type: "bigint", Value: float64(i)},
							{Name: "name", Type: "text", Value: fmt.Sprintf("row_%d", i)},
						},
					})
				}
				queries := a.buildBulkInsertQueries(events, emptySchemaInfo())
				require.Len(t, queries, 1)
				return queries
			},
			wantSQL: "ON CONFLICT",
			assert: func(t *testing.T, conn pglib.Querier) {
				requireCount(t, conn, "SELECT count(*) FROM bulk_insert_conflict", 50)
				// the conflicting row is kept, not overwritten, and the rest of
				// the batch still landed
				requireNameAt(t, conn, "SELECT name FROM bulk_insert_conflict WHERE id = 1", "existing")
				requireNameAt(t, conn, "SELECT name FROM bulk_insert_conflict WHERE id = 50", "row_50")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, stmt := range tc.setup {
				_, err := conn.Exec(ctx, stmt)
				require.NoError(t, err, stmt)
			}

			adapter, err := newDMLAdapter(tc.onConflictAction, false, loglib.NewNoopLogger())
			require.NoError(t, err)

			queries := tc.build(t, adapter)
			for _, q := range queries {
				require.Contains(t, q.sql, tc.wantSQL)
				_, err := conn.Exec(ctx, q.sql, q.args...)
				require.NoError(t, err, "postgres rejected the generated query: %s", q.sql)
			}

			tc.assert(t, conn)
		})
	}
}

func emptySchemaInfo() schemaInfo {
	return schemaInfo{
		generatedColumns:      map[string]struct{}{},
		alwaysIdentityColumns: map[string]struct{}{},
		sequenceColumns:       map[string]string{},
		enumColumns:           map[string]enumColumn{},
	}
}

func requireCount(t *testing.T, conn pglib.Querier, query string, want int) {
	t.Helper()

	var got int
	require.NoError(t, conn.QueryRow(context.Background(), []any{&got}, query))
	require.Equal(t, want, got, query)
}

func requireNameAt(t *testing.T, conn pglib.Querier, query, want string) {
	t.Helper()

	var got string
	require.NoError(t, conn.QueryRow(context.Background(), []any{&got}, query))
	require.Equal(t, want, got, query)
}
