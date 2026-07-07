// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

const (
	// schemaColumnTypesQuery lists every column of every user table together with
	// the resolved base type of the column.
	schemaColumnTypesQuery = `
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
	// sourceExtensionsQuery lists every extension installed on the database together
	// with the schema it was installed into.
	sourceExtensionsQuery = `
SELECT e.extname, n.nspname
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
ORDER BY e.extname
`

	targetExtensionsQuery = `SELECT extname FROM pg_extension`
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

// SchemaExtensionCompatibilityCheck verifies that every extension installed on
// the source database is also installed on the Postgres target. pgstream
// replicates the schema and data of objects that depend on extensions (custom
// types, functions, operators, index access methods), but it never installs the
// extensions themselves. It reports the full set of source extensions it
// inspected via Details (source_extensions), regardless of the outcome.
type SchemaExtensionCompatibilityCheck struct {
	Source postgres.AcquireFunc
	Target postgres.AcquireFunc

	// sourceExtensions is the set of extensions installed on the source, captured
	// during Run and surfaced through Details.
	sourceExtensions []string
}

func (c *SchemaExtensionCompatibilityCheck) Name() string { return "schema_extension_compatibility" }

func (c *SchemaExtensionCompatibilityCheck) Run(ctx context.Context) ([]Finding, error) {
	target, err := c.Target(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to target: %w", err)
	}
	targetExtensions, err := c.installedTargetExtensions(ctx, target)
	if err != nil {
		return nil, err
	}

	source, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}
	rows, err := source.Query(ctx, sourceExtensionsQuery)
	if err != nil {
		return nil, fmt.Errorf("querying source extensions: %w", err)
	}
	defer rows.Close()

	var missing []missingExtension
	for rows.Next() {
		var ext missingExtension
		if err := rows.Scan(&ext.name, &ext.schema); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		c.sourceExtensions = append(c.sourceExtensions, ext.name)
		if _, ok := targetExtensions[ext.name]; ok {
			continue
		}
		missing = append(missing, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}
	if len(missing) == 0 {
		return nil, nil
	}
	return []Finding{{Message: missingExtensionsMessage(missing)}}, nil
}

// Details exposes every extension installed on the source as a string array
// under source_extensions, so the report records what was inspected even when
// nothing is missing.
func (c *SchemaExtensionCompatibilityCheck) Details() map[string]any {
	names := c.sourceExtensions
	if names == nil {
		names = []string{}
	}
	return map[string]any{"source_extensions": names}
}

func (c *SchemaExtensionCompatibilityCheck) installedTargetExtensions(ctx context.Context, target postgres.Querier) (map[string]struct{}, error) {
	rows, err := target.Query(ctx, targetExtensionsQuery)
	if err != nil {
		return nil, fmt.Errorf("querying target extensions: %w", err)
	}
	defer rows.Close()

	installed := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning target row: %w", err)
		}
		installed[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating target rows: %w", err)
	}
	return installed, nil
}

type missingExtension struct {
	name   string
	schema string
}

// missingExtensionsMessage renders a single remediation covering every source
// extension absent from the target.
func missingExtensionsMessage(missing []missingExtension) string {
	names := make([]string, len(missing))
	stmts := make([]string, len(missing))
	for i, ext := range missing {
		names[i] = fmt.Sprintf("%q (source schema %q)", ext.name, ext.schema)
		stmts[i] = fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", postgres.QuoteIdentifier(ext.name))
	}
	noun := "extension"
	if len(missing) > 1 {
		noun = "extensions"
	}
	return fmt.Sprintf(
		"%d %s installed on source but missing on the target: %s; run the following on the target before migrating, or the schema will fail to apply: %s",
		len(missing), noun, strings.Join(names, ", "), strings.Join(stmts, " "),
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
