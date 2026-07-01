// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// SourceTableSelectPrivilegesCheck verifies that the source Postgres role can
// read every table pgstream may need to snapshot or replicate.
type SourceTableSelectPrivilegesCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *SourceTableSelectPrivilegesCheck) Name() string {
	return "source_table_select_privileges"
}

// SourceSequenceSelectPrivilegesCheck verifies that the source Postgres role
// can read every in-scope sequence pgstream may need to snapshot.
type SourceSequenceSelectPrivilegesCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *SourceSequenceSelectPrivilegesCheck) Name() string {
	return "source_sequence_select_privileges"
}

const sourceTableSelectPrivilegesQuery = `
SELECT
  current_user,
  n.nspname,
  c.relname,
  has_table_privilege(c.oid, 'SELECT') AS has_select
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
  AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY n.nspname, c.relname
`

const sourceSequenceSelectPrivilegesQuery = `
SELECT
  current_user,
  tn.nspname AS table_schema,
  t.relname AS table_name,
  sn.nspname AS sequence_schema,
  s.relname AS sequence_name,
  has_sequence_privilege(s.oid, 'SELECT') AS has_select,
  false AS is_standalone
FROM pg_class t
JOIN pg_namespace tn ON tn.oid = t.relnamespace
JOIN pg_attribute a ON a.attrelid = t.oid
JOIN pg_depend d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
JOIN pg_class s ON s.oid = d.objid
JOIN pg_namespace sn ON sn.oid = s.relnamespace
WHERE t.relkind IN ('r', 'p')
  AND s.relkind = 'S'
  AND d.deptype IN ('a', 'i')
  AND tn.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
  AND tn.nspname NOT LIKE 'pg_toast%'
  AND a.attnum > 0
  AND NOT a.attisdropped
UNION ALL
SELECT
  current_user,
  n.nspname AS table_schema,
  '' AS table_name,
  n.nspname AS sequence_schema,
  c.relname AS sequence_name,
  has_sequence_privilege(c.oid, 'SELECT') AS has_select,
  true AS is_standalone
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'S'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
  AND n.nspname NOT LIKE 'pg_toast%'
  AND NOT EXISTS (
    SELECT 1
    FROM pg_depend d
    JOIN pg_class t ON t.oid = d.refobjid
    WHERE d.objid = c.oid
      AND d.refobjsubid > 0
      AND d.deptype IN ('a', 'i')
      AND t.relkind IN ('r', 'p')
  )
ORDER BY table_schema, table_name, sequence_schema, sequence_name
`

func (c *SourceTableSelectPrivilegesCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, sourceTableSelectPrivilegesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying source table privileges: %w", err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var row sourceTableSelectPrivilegeRow
		if err := rows.Scan(&row.Role, &row.Schema, &row.Table, &row.HasSelect); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !c.Selection.IsTableInScope(row.Schema, row.Table) {
			continue
		}
		if !row.HasSelect {
			findings = append(findings, Finding{Message: sourceTableSelectPrivilegeMessage(row)})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

func (c *SourceSequenceSelectPrivilegesCheck) Run(ctx context.Context) ([]Finding, error) {
	include, exclude, err := selectionMaps(c.Selection)
	if err != nil {
		return nil, fmt.Errorf("parsing table selection: %w", err)
	}

	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, sourceSequenceSelectPrivilegesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying source sequence privileges: %w", err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var row sourceSequenceSelectPrivilegeRow
		if err := rows.Scan(&row.Role, &row.TableSchema, &row.Table, &row.SequenceSchema, &row.Sequence, &row.HasSelect, &row.IsStandalone); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !sequenceInScope(row, c.Selection, include, exclude) {
			continue
		}
		if !row.HasSelect {
			findings = append(findings, Finding{Message: sourceSequenceSelectPrivilegeMessage(row)})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

type sourceTableSelectPrivilegeRow struct {
	Role      string
	Schema    string
	Table     string
	HasSelect bool
}

type sourceSequenceSelectPrivilegeRow struct {
	Role           string
	TableSchema    string
	Table          string
	SequenceSchema string
	Sequence       string
	HasSelect      bool
	IsStandalone   bool
}

func selectionMaps(selection stream.TableSelection) (include, exclude postgres.SchemaTableMap, err error) {
	if len(selection.Include()) > 0 {
		include, err = postgres.NewSchemaTableMap(selection.Include())
		if err != nil {
			return nil, nil, fmt.Errorf("include: %w", err)
		}
	}
	if len(selection.Exclude()) > 0 {
		exclude, err = postgres.NewSchemaTableMap(selection.Exclude())
		if err != nil {
			return nil, nil, fmt.Errorf("exclude: %w", err)
		}
	}
	return include, exclude, nil
}

func sequenceInScope(row sourceSequenceSelectPrivilegeRow, selection stream.TableSelection, include, exclude postgres.SchemaTableMap) bool {
	if !row.IsStandalone {
		return selection.IsTableInScope(row.TableSchema, row.Table)
	}
	return standaloneSequenceSchemaInScope(row.SequenceSchema, selection, include, exclude)
}

func standaloneSequenceSchemaInScope(schema string, selection stream.TableSelection, include, exclude postgres.SchemaTableMap) bool {
	if selection.IsUnfiltered() {
		return true
	}
	if include != nil {
		return schemaHasWildcard(include, schema)
	}
	if exclude != nil {
		return !schemaHasWildcard(exclude, schema)
	}
	return true
}

func schemaHasWildcard(tables postgres.SchemaTableMap, schema string) bool {
	schemaTables := tables.GetSchemaTables(schema)
	if len(schemaTables) == 0 {
		return false
	}
	_, found := schemaTables["*"]
	return found
}

func sourceTableSelectPrivilegeMessage(row sourceTableSelectPrivilegeRow) string {
	quotedTable := postgres.QuoteIdentifier(row.Schema) + "." + postgres.QuoteIdentifier(row.Table)
	quotedRole := postgres.QuoteIdentifier(row.Role)
	return fmt.Sprintf(
		"source role %q lacks SELECT on %s.%s; run GRANT SELECT ON TABLE %s TO %s",
		row.Role, row.Schema, row.Table, quotedTable, quotedRole,
	)
}

func sourceSequenceSelectPrivilegeMessage(row sourceSequenceSelectPrivilegeRow) string {
	quotedSequence := postgres.QuoteIdentifier(row.SequenceSchema) + "." + postgres.QuoteIdentifier(row.Sequence)
	quotedRole := postgres.QuoteIdentifier(row.Role)
	return fmt.Sprintf(
		"source role %q lacks SELECT on sequence %s.%s; run GRANT SELECT ON SEQUENCE %s TO %s",
		row.Role, row.SequenceSchema, row.Sequence, quotedSequence, quotedRole,
	)
}
