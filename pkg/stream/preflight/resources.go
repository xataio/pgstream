// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// tableSizesQuery reports the table and index size in bytes of every user
// table, largest table first.
const tableSizesQuery = `
WITH RECURSIVE partition_tree AS (
  SELECT c.oid AS root, c.oid AS relid
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'p')
    AND NOT c.relispartition
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
    AND n.nspname NOT LIKE 'pg_toast%'
  UNION ALL
  SELECT t.root, partition.oid
  FROM partition_tree t
  JOIN pg_inherits i ON i.inhparent = t.relid
  JOIN pg_class partition ON partition.oid = i.inhrelid AND partition.relispartition
), sizes AS (
  SELECT
    root,
    sum(pg_table_size(relid))::bigint AS size_bytes,
    sum(pg_indexes_size(relid))::bigint AS index_bytes
  FROM partition_tree
  GROUP BY root
)
SELECT
  n.nspname AS schema_name,
  c.relname AS table_name,
  s.size_bytes,
  s.index_bytes
FROM sizes s
JOIN pg_class c ON c.oid = s.root
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY s.size_bytes DESC, schema_name, table_name
`

// TableSizesCheck reports the on-disk table and index size of each configured
// table. It is purely informational. The sizes are surfaced through Details,
// which means the JSON report only.
//
// The builder only instantiates it when a table selection is configured.
type TableSizesCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection

	tables          []tableSize
	totalBytes      int64
	totalIndexBytes int64
}

type tableSize struct {
	schema     string
	table      string
	bytes      int64
	indexBytes int64
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
		if err := rows.Scan(&t.schema, &t.table, &t.bytes, &t.indexBytes); err != nil {
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

func (c *TableSizesCheck) Summary() string {
	return fmt.Sprintf("%d tables · %s + %s indexes",
		len(c.tables), prettySize(c.totalBytes), prettySize(c.totalIndexBytes))
}

// ExpandedSummary lists every in-scope table behind the summary, largest first,
// closing with the totals. Rendered only in verbose mode.
func (c *TableSizesCheck) ExpandedSummary() []string {
	if len(c.tables) == 0 {
		return nil
	}
	width := 0
	for _, t := range c.tables {
		if n := len(t.schema) + len(t.table) + 1; n > width {
			width = n
		}
	}
	if n := len(tableSizesTotalLabel); n > width {
		width = n
	}

	rows := make([]string, 0, len(c.tables)+1)
	for _, t := range c.tables {
		rows = append(rows, fmt.Sprintf("%-*s  %10s  indexes %s",
			width, t.schema+"."+t.table, prettySize(t.bytes), prettySize(t.indexBytes)))
	}
	return append(rows, fmt.Sprintf("%-*s  %10s  indexes %s",
		width, tableSizesTotalLabel, prettySize(c.totalBytes), prettySize(c.totalIndexBytes)))
}

const tableSizesTotalLabel = "total"

func (c *TableSizesCheck) Details() map[string]any {
	tables := make([]map[string]any, 0, len(c.tables))
	for _, t := range c.tables {
		tables = append(tables, map[string]any{
			"schema":           t.schema,
			"table":            t.table,
			"size_bytes":       t.bytes,
			"index_size_bytes": t.indexBytes,
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
				c.Demand, available, maxConns, reserved, used,
			),
		}}, nil
	}
	return nil, nil
}
