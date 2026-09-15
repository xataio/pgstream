// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// ReplicaIdentityCheck verifies that every in-scope table has a REPLICA
// IDENTITY sufficient for logical replication of UPDATE/DELETE WAL events.
// Anything insufficient means those events would silently be skipped at run
// time. The check inspects only the tables that pass the user's
// include/exclude filter (TableSelection) so unrelated tables don't pollute
// the report.
type ReplicaIdentityCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *ReplicaIdentityCheck) Name() string { return "replica_identity" }

const replicaIdentityQuery = `
SELECT
  n.nspname,
  c.relname,
  c.relreplident::text,
  pk.indrelid IS NOT NULL AS has_pk,
  ri.indrelid IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM pg_attribute a
    WHERE a.attrelid = c.oid
      AND a.attnum = ANY (ri.indkey::int[])
      AND NOT a.attnotnull
  ) AS replident_index_ok
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_index pk ON pk.indrelid = c.oid AND pk.indisprimary
LEFT JOIN pg_index ri ON ri.indrelid = c.oid
                     AND ri.indisreplident
                     AND ri.indisvalid
                     AND ri.indisunique
                     AND ri.indpred IS NULL
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
ORDER BY n.nspname, c.relname
`

func (c *ReplicaIdentityCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, replicaIdentityQuery)
	if err != nil {
		return nil, fmt.Errorf("querying pg_class for replica identity: %w", err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var t replicaIdentityRow
		if err := rows.Scan(&t.Schema, &t.Name, &t.Relreplident, &t.HasPK, &t.ReplidentOK); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !c.Selection.IsTableInScope(t.Schema, t.Name) {
			continue
		}
		if finding, found := assessReplicaIdentity(t); found {
			findings = append(findings, finding)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

type replicaIdentityRow struct {
	Schema       string
	Name         string
	Relreplident string // 'd' default, 'n' nothing, 'f' full, 'i' index
	HasPK        bool
	ReplidentOK  bool // only meaningful when Relreplident == 'i'
}

// assessReplicaIdentity returns a finding if the table's REPLICA IDENTITY is
// insufficient for UPDATE/DELETE replication, and reports false if it's OK.
// Kept pure for cheap unit testing.
func assessReplicaIdentity(t replicaIdentityRow) (Finding, bool) {
	tbl := postgres.QuoteIdentifier(t.Schema) + "." + postgres.QuoteIdentifier(t.Name)
	switch t.Relreplident {
	case "f":
		return Finding{}, false
	case "d":
		if t.HasPK {
			return Finding{}, false
		}
		return Finding{
			ID:      FindingIDReplicaIdentityNoPrimaryKey,
			Title:   "A table has replica identity default and no primary key",
			Detail:  fmt.Sprintf("The table %s has REPLICA IDENTITY=default and no PRIMARY KEY, so Postgres skips its UPDATE and DELETE WAL events. Add a PRIMARY KEY, set REPLICA IDENTITY FULL, or set REPLICA IDENTITY USING INDEX with a unique, non-partial, NOT NULL index.", tbl),
			Message: fmt.Sprintf("%s: REPLICA IDENTITY=default but no PRIMARY KEY; UPDATE/DELETE WAL events will be skipped — add a PRIMARY KEY, set REPLICA IDENTITY FULL, or REPLICA IDENTITY USING INDEX <unique non-partial NOT-NULL index>", tbl),
		}, true
	case "n":
		return Finding{
			ID:      FindingIDReplicaIdentityNothing,
			Title:   "A table has replica identity nothing",
			Detail:  fmt.Sprintf("The table %s has REPLICA IDENTITY=nothing, so Postgres skips its UPDATE and DELETE WAL events. Set REPLICA IDENTITY DEFAULT, FULL, or USING INDEX.", tbl),
			Message: fmt.Sprintf("%s: REPLICA IDENTITY=nothing; UPDATE/DELETE WAL events will be skipped — set REPLICA IDENTITY DEFAULT / FULL / USING INDEX", tbl),
		}, true
	case "i":
		if t.ReplidentOK {
			return Finding{}, false
		}
		return Finding{
			ID:      FindingIDReplicaIdentityIndexUnusable,
			Title:   "A table uses an unusable index as its replica identity",
			Detail:  fmt.Sprintf("The table %s has REPLICA IDENTITY=index, but the index is invalid, non-unique, partial, or includes nullable columns. Select a different index, or use REPLICA IDENTITY FULL.", tbl),
			Message: fmt.Sprintf("%s: REPLICA IDENTITY=index but the chosen index is invalid, non-unique, partial, or includes nullable columns — pick a different index or use REPLICA IDENTITY FULL", tbl),
		}, true
	default:
		return Finding{
			ID:      FindingIDReplicaIdentityUnknown,
			Title:   "A table has an unknown replica identity",
			Detail:  fmt.Sprintf("The table %s reports REPLICA IDENTITY=%q, which this Postgres version does not define. Set REPLICA IDENTITY DEFAULT, FULL, or USING INDEX.", tbl, t.Relreplident),
			Message: fmt.Sprintf("%s: unknown REPLICA IDENTITY=%q on this Postgres version", tbl, t.Relreplident),
		}, true
	}
}
