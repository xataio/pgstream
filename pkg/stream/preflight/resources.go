// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// tableSizesQuery reports the table and index size of every user table,
// largest table first.
//
// The two sizes stay separate because they answer different questions: the
// data snapshot copies table data, while indexes are rebuilt on the target
// from the schema dump. Table size predicts copy volume, index size predicts
// target disk headroom and post-restore rebuild cost, so pg_total_relation_size
// (which merges them) would lose that.
//
// Table size is pg_table_size (heap plus TOAST, without indexes); index size is
// pg_indexes_size. Declarative partitions are rolled into their root parent
// rather than listed separately: a partitioned parent stores nothing itself, so
// both functions return zero on it — reporting that would show an empty table
// for the very name the user configured — and listing parent and partitions
// side by side would double count the totals. Legacy inheritance children are
// not partitions, so they stay standalone rows.
//
// pg_inherits recursion rather than pg_partition_tree, which needs PostgreSQL
// 12 and pgstream supports 10+.
const tableSizesQuery = `
WITH RECURSIVE partition_tree AS (
  SELECT c.oid AS root, c.oid AS relid
  FROM pg_class c
  WHERE c.relkind = 'p'
  UNION ALL
  SELECT t.root, child.oid
  FROM partition_tree t
  JOIN pg_inherits i ON i.inhparent = t.relid
  JOIN pg_class child ON child.oid = i.inhrelid AND child.relispartition
), partition_sizes AS (
  SELECT
    root,
    sum(pg_table_size(relid))::bigint AS table_bytes,
    sum(pg_indexes_size(relid))::bigint AS index_bytes
  FROM partition_tree
  GROUP BY root
)
SELECT
  schema_name,
  table_name,
  size_bytes,
  pg_size_pretty(size_bytes) AS size,
  index_bytes,
  pg_size_pretty(index_bytes) AS index_size
FROM (
  SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE
      WHEN c.relkind = 'p' THEN COALESCE(ps.table_bytes, 0)
      ELSE pg_table_size(c.oid)
    END::bigint AS size_bytes,
    CASE
      WHEN c.relkind = 'p' THEN COALESCE(ps.index_bytes, 0)
      ELSE pg_indexes_size(c.oid)
    END::bigint AS index_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN partition_sizes ps ON ps.root = c.oid
  WHERE c.relkind IN ('r', 'p')
    AND NOT c.relispartition
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
    AND n.nspname NOT LIKE 'pg_toast%'
) s
ORDER BY size_bytes DESC, schema_name, table_name
`

// TableSizesCheck reports the on-disk table and index size of each configured
// table. It is purely informational — no size is wrong on its own — so it never
// produces a finding; the sizes are surfaced through Details, which means the
// JSON report only.
//
// The builder only instantiates it when a table selection is configured.
// Reporting every table of an unfiltered database would be a catalog dump, not
// a preflight result.
type TableSizesCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection

	// tables and the totals are captured during Run and surfaced through
	// Details. Empty until Run has read them.
	tables          []tableSize
	totalBytes      int64
	totalIndexBytes int64
}

type tableSize struct {
	schema      string
	table       string
	bytes       int64
	pretty      string
	indexBytes  int64
	indexPretty string
}

func (c *TableSizesCheck) Name() string { return "table_sizes" }

func (c *TableSizesCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, tableSizesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying table sizes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t tableSize
		if err := rows.Scan(&t.schema, &t.table, &t.bytes, &t.pretty, &t.indexBytes, &t.indexPretty); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !c.Selection.IsTableInScope(t.schema, t.table) {
			continue
		}
		c.tables = append(c.tables, t)
		c.totalBytes += t.bytes
		c.totalIndexBytes += t.indexBytes
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return nil, nil
}

// Details exposes the in-scope tables largest first, alongside their totals, so
// the report records what the run was sizing itself against.
func (c *TableSizesCheck) Details() map[string]any {
	tables := make([]map[string]any, 0, len(c.tables))
	for _, t := range c.tables {
		tables = append(tables, map[string]any{
			"schema":           t.schema,
			"table":            t.table,
			"size_bytes":       t.bytes,
			"size":             t.pretty,
			"index_size_bytes": t.indexBytes,
			"index_size":       t.indexPretty,
		})
	}
	return map[string]any{
		"tables":             tables,
		"tables_size_bytes":  c.totalBytes,
		"indexes_size_bytes": c.totalIndexBytes,
	}
}

// SnapshotConnectionsCheck verifies the source Postgres has enough spare
// connection slots to serve the snapshot's peak concurrency
// (snapshot_workers × table_workers) on top of what is already in use, without
// exceeding max_connections. Non-superuser roles cannot use the slots reserved
// by superuser_reserved_connections, so those are excluded from the headroom.
type SnapshotConnectionsCheck struct {
	Source postgres.AcquireFunc
	// Demand is the number of concurrent connections the snapshot will open at
	// peak (snapshot_workers × table_workers).
	Demand uint
}

func (c *SnapshotConnectionsCheck) Name() string { return "snapshot_connection_headroom" }

func (c *SnapshotConnectionsCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	var maxConns, reserved, used int
	err = conn.QueryRow(ctx, []any{&maxConns, &reserved, &used}, `
		SELECT
		  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections'),
		  (SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections'),
		  (SELECT count(*)::int FROM pg_stat_activity)
	`)
	if err != nil {
		return nil, fmt.Errorf("querying connection limits: %w", err)
	}

	available := maxConns - reserved - used
	if available < 0 {
		available = 0
	}
	if int(c.Demand) > available {
		return []Finding{{
			Message: fmt.Sprintf(
				"snapshot needs %d concurrent connections (snapshot_workers × table_workers) but source has only %d available (max_connections=%d, superuser_reserved_connections=%d, %d in use); lower snapshot_workers/table_workers, raise max_connections (requires restart), or reduce existing connections",
				c.Demand, available, maxConns, reserved, used),
		}}, nil
	}
	return nil, nil
}
