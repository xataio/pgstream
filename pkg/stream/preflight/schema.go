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
// every in-scope table. pgstream moves column values through pgx's type system
// (for both snapshot COPY and WAL replication) and has no bespoke handling for
// user-defined types, so a column whose type pgx does not natively recognize has
// no guaranteed encode/decode path — its values may be dropped or fail to write
// to the target. Such columns are reported as blocking findings.
//
// Domains are resolved to their underlying base type before the check, so a
// domain over a supported built-in (the common case) is not flagged. The check
// inspects only the tables that pass the user's include/exclude filter
// (TableSelection) so unrelated tables don't pollute the report.
type SchemaTypeCompatibilityCheck struct {
	Source    postgres.AcquireFunc
	Selection stream.TableSelection
}

func (c *SchemaTypeCompatibilityCheck) Name() string { return "schema_type_compatibility" }

// schemaColumnTypesQuery lists every column of every user table together with
// the resolved base type of the column. The recursive CTE walks domain chains
// (domain over domain over … over base) down to the first non-domain type, so
// the reported OID/name/kind always describe a concrete type pgstream would have
// to decode.
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

	// pgx's static type map is the source of truth for what pgstream can encode
	// and decode; it's resolved in-memory, so no further queries are needed.
	typeMap := pgtype.NewMap()

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
		findings = append(findings, Finding{Message: unsupportedColumnTypeMessage(row)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	return findings, nil
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
		"%s: %s is not supported by pgstream's value pipeline; pgstream relies on pgx to encode and decode column values and has no native handling for it — cast the column to a supported built-in type or exclude the table from the migration",
		col, desc,
	)
}

// PostgresRangeTypeCheck verifies that every in-scope range/multirange column
// uses a range type pgstream's Postgres target writer can actually encode.
// pgstream normalizes only int4range, int8range, and tstzrange values before
// INSERT/COPY (getTypedRangeValue in the postgres processor); every other range
// type (numrange, tsrange, daterange, …) and all multirange types reach pgx as
// an untyped pgtype.Range[any], which pgx cannot encode to the target column, so
// the write fails. This is a Postgres-target-specific limitation, so the builder
// only wires this check in when the target is Postgres. It shares the column
// query (and domain resolution) with SchemaTypeCompatibilityCheck.
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

// unsupportedRangeTypeMessage returns a finding message when the column is a
// range/multirange type the Postgres target writer can't encode, or "" when the
// column is fine (not a range, or one of the supported range types). Kept pure
// for cheap unit testing.
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
		"%s: %s %q cannot be written to a Postgres target; pgstream only encodes int4range, int8range, and tstzrange values, so other range and multirange types fail on INSERT/COPY — cast the column to a supported range type or exclude the table from the migration",
		col, typeKindLabel(row.TypeKind), row.TypeName,
	)
}

// typeKindLabel turns a pg_type.typtype code into a human-readable noun for the
// finding message, or "" for an ordinary base type (where the bare type name
// reads better, e.g. a built-in system type pgx doesn't register like regclass).
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
