// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// SchemaTypeCompatibilityCheck verifies that pgstream can resolve a Postgres
// type for every column of every in-scope table. A column whose type cannot be
// resolved cannot be decoded from the WAL or a snapshot, so it is reported as a
// blocking finding before the first event runs. The check inspects only the
// tables that pass the user's include/exclude filter (TableSelection) so
// unrelated tables don't pollute the report.
type SchemaTypeCompatibilityCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *SchemaTypeCompatibilityCheck) Name() string { return "schema_type_compatibility" }

const schemaColumnTypesQuery = `
SELECT
  n.nspname,
  c.relname,
  a.attname,
  a.atttypid::bigint
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
  AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY n.nspname, c.relname, a.attnum
`

func (c *SchemaTypeCompatibilityCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	// Drain the column cursor fully before resolving types: the mapper may issue
	// its own query on the same connection for non-standard OIDs, which a still
	// open cursor would block.
	columns, err := func() ([]schemaColumnRow, error) {
		rows, err := conn.Query(ctx, schemaColumnTypesQuery)
		if err != nil {
			return nil, fmt.Errorf("querying source column types: %w", err)
		}
		defer rows.Close()

		var columns []schemaColumnRow
		for rows.Next() {
			var row schemaColumnRow
			if err := rows.Scan(&row.Schema, &row.Table, &row.Column, &row.TypeOID); err != nil {
				return nil, fmt.Errorf("scanning row: %w", err)
			}
			if !c.Selection.IsTableInScope(row.Schema, row.Table) {
				continue
			}
			columns = append(columns, row)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating rows: %w", err)
		}
		return columns, nil
	}()
	if err != nil {
		return nil, err
	}

	mapper := postgres.NewMapper(conn)
	var findings []Finding
	for _, row := range columns {
		if _, err := mapper.TypeForOID(ctx, uint32(row.TypeOID)); err != nil {
			findings = append(findings, Finding{Message: schemaColumnTypeMessage(row, err)})
		}
	}
	return findings, nil
}

type schemaColumnRow struct {
	Schema  string
	Table   string
	Column  string
	TypeOID int64
}

func schemaColumnTypeMessage(row schemaColumnRow, err error) string {
	return fmt.Sprintf(
		"column %s.%s.%s has unsupported type OID %d: %v — pgstream cannot decode this column; cast it to a supported type or exclude the table from the migration",
		row.Schema, row.Table, row.Column, row.TypeOID, err,
	)
}
