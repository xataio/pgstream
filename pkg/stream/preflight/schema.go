// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// SchemaTypeCompatibilityCheck verifies that pgstream can decode every column of
// every in-scope table. A column type is considered supported when either pgx's
// static type map natively handles it, or pgstream adds its own handling on top
// of pgx (pgstreamSupportedTypes).
type SchemaTypeCompatibilityCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *SchemaTypeCompatibilityCheck) Name() string { return "schema_type_compatibility" }

// schemaColumnTypesQuery lists every column of every user table together with
// the resolved base type of the column.
const schemaColumnTypesQuery = `
WITH RECURSIVE type_resolved AS (
    SELECT oid AS source_oid, oid AS resolved_oid, typtype, typbasetype, typname
    FROM pg_type
  UNION ALL
    SELECT tr.source_oid, b.oid, b.typtype, b.typbasetype, b.typname
    FROM type_resolved tr
    JOIN pg_type b ON b.oid = tr.typbasetype
    WHERE tr.typtype = 'd'
)
SELECT
  n.nspname,
  c.relname,
  a.attname,
  tr.resolved_oid::bigint,
  tr.typname,
  tr.typtype
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN type_resolved tr ON tr.source_oid = a.atttypid AND tr.typtype <> 'd'
WHERE c.relkind IN ('r', 'p')
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgstream')
ORDER BY n.nspname, c.relname, a.attnum
`

func (c *SchemaTypeCompatibilityCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, schemaColumnTypesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying source column types: %w", err)
	}
	defer rows.Close()

	typeMap := pgtype.NewMap()
	pgstreamSupportedTypes := pgstreamSupportedTypes()

	var findings []Finding
	for rows.Next() {
		var row schemaColumnRow
		if err := rows.Scan(&row.Schema, &row.Table, &row.Column, &row.BaseOID, &row.TypeName, &row.TypeKind); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !c.Selection.IsTableInScope(row.Schema, row.Table) {
			continue
		}
		if _, ok := typeMap.TypeForOID(uint32(row.BaseOID)); ok {
			continue
		}
		if _, ok := pgstreamSupportedTypes[row.TypeName]; ok {
			continue
		}
		findings = append(findings, Finding{Message: unsupportedColumnTypeMessage(row)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

// pgstreamSupportedTypes is the set of PostgreSQL types that pgx's static type
// map has no codec for out of the box, but that pgstream still supports because
// it registers a codec for them on every connection.
func pgstreamSupportedTypes() map[string]struct{} {
	names := postgres.ExtensionTypeNames()
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}

type schemaColumnRow struct {
	Schema   string
	Table    string
	Column   string
	BaseOID  int64  // OID of the column's type, with domains resolved to their base
	TypeName string // name of that resolved type
	TypeKind string // pg_type.typtype of the resolved type
}

func unsupportedColumnTypeMessage(row schemaColumnRow) string {
	col := postgres.QuoteIdentifier(row.Schema) + "." + postgres.QuoteIdentifier(row.Table) + "." + postgres.QuoteIdentifier(row.Column)
	desc := fmt.Sprintf("type %q", row.TypeName)
	if kind := typeKindLabel(row.TypeKind); kind != "" {
		desc = fmt.Sprintf("%s %q", kind, row.TypeName)
	}
	return fmt.Sprintf(
		"%s: %s unknown type; Cast the column to a supported type or exclude the table from the migration. To request support, open an issue in the repo: https://github.com/xataio/pgstream/issues/new",
		col, desc,
	)
}

// PostgresRangeTypeCheck verifies that every in-scope range/multirange column
// uses a range type pgstream's Postgres target writer can actually encode.
type PostgresRangeTypeCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *PostgresRangeTypeCheck) Name() string { return "postgres_range_type_support" }

// supportedPostgresRangeTypes are the range types pgstream normalizes before
// writing to a Postgres target (see getTypedRangeValue in the postgres
// processor). Anything else with a range/multirange kind is unsupported.
var supportedPostgresRangeTypes = map[string]struct{}{
	"int4range": {},
	"int8range": {},
	"tstzrange": {},
}

func (c *PostgresRangeTypeCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	rows, err := conn.Query(ctx, schemaColumnTypesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying source column types: %w", err)
	}
	defer rows.Close()

	var findings []Finding
	for rows.Next() {
		var row schemaColumnRow
		if err := rows.Scan(&row.Schema, &row.Table, &row.Column, &row.BaseOID, &row.TypeName, &row.TypeKind); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		if !c.Selection.IsTableInScope(row.Schema, row.Table) {
			continue
		}
		if msg := unsupportedRangeTypeMessage(row); msg != "" {
			findings = append(findings, Finding{Message: msg})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
}

func unsupportedRangeTypeMessage(row schemaColumnRow) string {
	switch row.TypeKind {
	case "r", "m": // range, multirange
	default:
		return ""
	}
	if _, ok := supportedPostgresRangeTypes[row.TypeName]; ok {
		return ""
	}
	col := postgres.QuoteIdentifier(row.Schema) + "." + postgres.QuoteIdentifier(row.Table) + "." + postgres.QuoteIdentifier(row.Column)
	return fmt.Sprintf(
		"%s: %s %q unknown type; pgstream only encodes int4range, int8range, and tstzrange values. Cast the column to a supported type or exclude the table from the migration. To request support, open an issue in the repo: https://github.com/xataio/pgstream/issues/new",
		col, typeKindLabel(row.TypeKind), row.TypeName,
	)
}

func typeKindLabel(typtype string) string {
	switch typtype {
	case "c":
		return "composite type"
	case "e":
		return "enum type"
	case "r":
		return "range type"
	case "m":
		return "multirange type"
	case "p":
		return "pseudo-type"
	default:
		return ""
	}
}
