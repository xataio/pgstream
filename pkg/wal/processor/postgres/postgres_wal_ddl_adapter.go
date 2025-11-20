// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type ddlAdapter struct {
	schemalogQuerier schemalogQuerier
	schemaDiffer     schemaDiffer
}

type schemalogQuerier interface {
	Fetch(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error)
}

type schemaDiffer func(old, new *schemalog.LogEntry) *schemalog.Diff

type logEntryAdapter func(*wal.Data) (*schemalog.LogEntry, error)

func newDDLAdapter(querier schemalogQuerier) *ddlAdapter {
	return &ddlAdapter{
		schemalogQuerier: querier,
		schemaDiffer:     schemalog.ComputeSchemaDiff,
	}
}

func (a *ddlAdapter) schemaLogToQueries(ctx context.Context, schemaLog *schemalog.LogEntry) ([]*query, error) {
	var previousSchemaLog *schemalog.LogEntry
	if schemaLog.Version > 0 {
		var err error
		previousSchemaLog, err = a.schemalogQuerier.Fetch(ctx, schemaLog.SchemaName, int(schemaLog.Version)-1)
		if err != nil && !errors.Is(err, schemalog.ErrNoRows) {
			return nil, fmt.Errorf("fetching existing schema log entry: %w", err)
		}
	}

	diff := a.schemaDiffer(previousSchemaLog, schemaLog)

	queries := []*query{
		a.createSchemaIfNotExists(schemaLog.SchemaName),
	}

	schemaQueries, err := a.schemaDiffToQueries(schemaLog.SchemaName, diff)
	if err != nil {
		return nil, err
	}

	return append(queries, schemaQueries...), nil
}

const createSchemaIfNotExistsQuery = "CREATE SCHEMA IF NOT EXISTS %s"

func (a *ddlAdapter) createSchemaIfNotExists(schemaName string) *query {
	createSchemaQuery := fmt.Sprintf(createSchemaIfNotExistsQuery, pglib.QuoteIdentifier(schemaName))
	return a.newDDLQuery(schemaName, "", createSchemaQuery)
}

func (a *ddlAdapter) schemaDiffToQueries(schemaName string, diff *schemalog.Diff) ([]*query, error) {
	if diff.IsEmpty() {
		return []*query{}, nil
	}

	queries := []*query{}
	fkQueries := []*query{}
	for _, table := range diff.TablesRemoved {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTableName(schemaName, table.Name))
		queries = append(queries, a.newDDLQuery(schemaName, table.Name, dropQuery))
	}

	for _, table := range diff.TablesAdded {
		queries = append(queries, a.buildCreateTableQuery(schemaName, table))
		queries = append(queries, a.buildCreateIndexQueries(schemaName, table)...)
		queries = append(queries, a.buildAddConstraintQueries(schemaName, table)...)
		fkQueries = append(fkQueries, a.buildAddForeignKeyQueries(schemaName, table)...)
	}

	for _, tableDiff := range diff.TablesChanged {
		alterQueries, alterFKQueries := a.buildAlterTableQueries(schemaName, tableDiff)
		queries = append(queries, alterQueries...)
		fkQueries = append(fkQueries, alterFKQueries...)
	}

	return append(queries, fkQueries...), nil
}

func (a *ddlAdapter) buildCreateIndexQueries(schemaName string, table schemalog.Table) []*query {
	constraintIndexes := constraintBackedIndexNames(table.Constraints)
	queries := make([]*query, 0, len(table.Indexes))
	for _, idx := range table.Indexes {
		if idx.Definition == "" {
			continue
		}
		if _, skip := constraintIndexes[idx.Name]; skip {
			continue
		}
		createQuery := ensureIndexHasIfNotExists(idx.Definition)
		queries = append(queries, a.newDDLQuery(schemaName, table.Name, createQuery))
	}
	return queries
}

func (a *ddlAdapter) buildCreateTableQuery(schemaName string, table schemalog.Table) *query {
	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", quotedTableName(schemaName, table.Name))
	uniqueConstraints := make([]string, 0, len(table.Columns))
	columnDefinitions := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		columnDefinitions = append(columnDefinitions, a.buildColumnDefinition(&col))
		// if there's a unique constraint associated to the column, and it's not
		// the primary key, explicitly add it
		if uniqueConstraint := a.buildUniqueConstraint(col); uniqueConstraint != "" && !slices.Contains(table.PrimaryKeyColumns, col.Name) {
			uniqueConstraints = append(uniqueConstraints, uniqueConstraint)
		}
	}

	createQuery = fmt.Sprintf("%s\n%s", createQuery, strings.Join(columnDefinitions, ",\n"))
	if len(uniqueConstraints) > 0 {
		createQuery = fmt.Sprintf("%s,\n%s", createQuery, strings.Join(uniqueConstraints, ",\n"))
	}

	primaryKeys := make([]string, 0, len(table.PrimaryKeyColumns))
	for _, col := range table.PrimaryKeyColumns {
		primaryKeys = append(primaryKeys, pglib.QuoteIdentifier(col))
	}

	if len(primaryKeys) > 0 {
		createQuery = fmt.Sprintf("%s,\nPRIMARY KEY (%s)\n", createQuery, strings.Join(primaryKeys, ","))
	}

	createQuery += ")"

	return a.newDDLQuery(schemaName, table.Name, createQuery)
}

func (a *ddlAdapter) buildColumnDefinition(column *schemalog.Column) string {
	colDefinition := fmt.Sprintf("%s %s", pglib.QuoteIdentifier(column.Name), column.DataType)
	if !column.Nullable {
		colDefinition = fmt.Sprintf("%s NOT NULL", colDefinition)
	}
	// do not set default values with sequences and generated columns since they
	// must be aligned between source/target. Keep source database as source of
	// truth.
	if column.DefaultValue != nil && !strings.Contains(*column.DefaultValue, "seq") && !column.Generated {
		colDefinition = fmt.Sprintf("%s DEFAULT %s", colDefinition, *column.DefaultValue)
	}

	return colDefinition
}

func (a *ddlAdapter) buildUniqueConstraint(column schemalog.Column) string {
	if column.Unique {
		return fmt.Sprintf("UNIQUE (%s)", pglib.QuoteIdentifier(column.Name))
	}
	return ""
}

func (a *ddlAdapter) buildAlterTableQueries(schemaName string, tableDiff schemalog.TableDiff) ([]*query, []*query) {
	if tableDiff.IsEmpty() {
		return []*query{}, []*query{}
	}

	queries := []*query{}
	fkQueries := []*query{}
	if tableDiff.TableNameChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			quotedTableName(schemaName, tableDiff.TableNameChange.Old),
			tableDiff.TableNameChange.New,
		)
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, alterQuery))
	}

	for _, fk := range tableDiff.ForeignKeysRemoved {
		if dropQuery := buildDropConstraintQuery(schemaName, tableDiff.TableName, fk.Name); dropQuery != nil {
			queries = append(queries, dropQuery)
		}
	}

	for _, constraint := range tableDiff.ConstraintsRemoved {
		if dropQuery := buildDropConstraintQuery(schemaName, tableDiff.TableName, constraint.Name); dropQuery != nil {
			queries = append(queries, dropQuery)
		}
	}

	for _, col := range tableDiff.ColumnsRemoved {
		alterQuery := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quotedTableName(schemaName, tableDiff.TableName), pglib.QuoteIdentifier(col.Name))
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, alterQuery))
	}

	for _, col := range tableDiff.ColumnsAdded {
		alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quotedTableName(schemaName, tableDiff.TableName), a.buildColumnDefinition(&col))
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, alterQuery))
	}

	for _, colDiff := range tableDiff.ColumnsChanged {
		alterQueries := a.buildAlterColumnQueries(schemaName, tableDiff.TableName, &colDiff)
		queries = append(queries, alterQueries...)
	}

	for _, idx := range tableDiff.IndexesRemoved {
		dropQuery := buildDropIndexQuery(schemaName, idx.Name)
		if dropQuery == "" {
			continue
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, dropQuery))
	}

	constraintIndexes := constraintBackedIndexNames(tableDiff.ConstraintsAdded)
	for _, idx := range tableDiff.IndexesAdded {
		if idx.Definition == "" {
			continue
		}
		if _, skip := constraintIndexes[idx.Name]; skip {
			continue
		}
		createQuery := ensureIndexHasIfNotExists(idx.Definition)
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, createQuery))
	}

	for _, definition := range tableDiff.IndexesChanged {
		stmt := strings.TrimSpace(definition)
		if stmt == "" {
			continue
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, stmt))
	}

	for _, constraint := range tableDiff.ConstraintsAdded {
		if addQuery := buildAddConstraintQuery(schemaName, tableDiff.TableName, constraint); addQuery != nil {
			queries = append(queries, addQuery)
		}
	}

	for _, fk := range tableDiff.ForeignKeysAdded {
		if addQuery := buildAddForeignKeyQuery(schemaName, tableDiff.TableName, fk); addQuery != nil {
			fkQueries = append(fkQueries, addQuery)
		}
	}

	return queries, fkQueries
}

func (a *ddlAdapter) buildAlterColumnQueries(schemaName, tableName string, columnDiff *schemalog.ColumnDiff) []*query {
	if columnDiff.IsEmpty() {
		return []*query{}
	}

	queries := []*query{}
	if columnDiff.NameChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
			quotedTableName(schemaName, tableName),
			pglib.QuoteIdentifier(columnDiff.NameChange.Old),
			pglib.QuoteIdentifier(columnDiff.NameChange.New),
		)
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	if columnDiff.TypeChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
			quotedTableName(schemaName, tableName),
			pglib.QuoteIdentifier(columnDiff.ColumnName),
			columnDiff.TypeChange.New,
		)
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	if columnDiff.NullChange != nil {
		alterQuery := ""
		switch {
		// from not nullable to nullable
		case columnDiff.NullChange.New:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		default:
			// from nullable to not nullable
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	if columnDiff.DefaultChange != nil {
		alterQuery := ""
		switch columnDiff.DefaultChange.New {
		// removing the default
		case nil:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		default:
			// do not set default values with sequences since they will differ between
			// source/target. Keep source database as source of truth.
			if !strings.Contains(*columnDiff.DefaultChange.New, "seq") {
				alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
					quotedTableName(schemaName, tableName),
					pglib.QuoteIdentifier(columnDiff.ColumnName),
					*columnDiff.DefaultChange.New,
				)
			}
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	// TODO: add support for unique constraint changes

	return queries
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}

func (a *ddlAdapter) buildAddConstraintQueries(schemaName string, table schemalog.Table) []*query {
	queries := make([]*query, 0, len(table.Constraints))
	for _, constraint := range table.Constraints {
		if q := buildAddConstraintQuery(schemaName, table.Name, constraint); q != nil {
			queries = append(queries, q)
		}
	}
	return queries
}

func (a *ddlAdapter) buildAddForeignKeyQueries(schemaName string, table schemalog.Table) []*query {
	queries := make([]*query, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		if q := buildAddForeignKeyQuery(schemaName, table.Name, fk); q != nil {
			queries = append(queries, q)
		}
	}
	return queries
}

func ensureIndexHasIfNotExists(definition string) string {
	switch {
	case strings.HasPrefix(definition, "CREATE UNIQUE INDEX "):
		return strings.Replace(definition, "CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
	case strings.HasPrefix(definition, "CREATE INDEX "):
		return strings.Replace(definition, "CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
	default:
		return definition
	}
}

func constraintBackedIndexNames(constraints []schemalog.Constraint) map[string]struct{} {
	indexes := make(map[string]struct{}, len(constraints))
	for _, constraint := range constraints {
		if constraintCreatesIndex(constraint.Type) && constraint.Name != "" {
			indexes[constraint.Name] = struct{}{}
		}
	}
	return indexes
}

func constraintCreatesIndex(constraintType string) bool {
	switch {
	case strings.EqualFold(constraintType, "UNIQUE"),
		strings.EqualFold(constraintType, "PRIMARY KEY"),
		strings.EqualFold(constraintType, "EXCLUDE"):
		return true
	default:
		return false
	}
}

func buildDropIndexQuery(schemaName, indexName string) string {
	if indexName == "" {
		return ""
	}
	qualified := pglib.QuoteQualifiedIdentifier(schemaName, indexName)
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", qualified)
}

func buildDropConstraintQuery(schemaName, tableName, constraintName string) *query {
	if constraintName == "" {
		return nil
	}
	dropQuery := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s",
		quotedTableName(schemaName, tableName),
		pglib.QuoteIdentifier(constraintName),
	)
	return &query{
		schema: schemaName,
		table:  tableName,
		sql:    dropQuery,
		isDDL:  true,
	}
}

func buildAddConstraintQuery(schemaName, tableName string, constraint schemalog.Constraint) *query {
	if constraint.Name == "" || constraint.Definition == "" {
		return nil
	}
	addQuery := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
		quotedTableName(schemaName, tableName),
		pglib.QuoteIdentifier(constraint.Name),
		constraint.Definition,
	)
	return &query{
		schema: schemaName,
		table:  tableName,
		sql:    addQuery,
		isDDL:  true,
	}
}

func buildAddForeignKeyQuery(schemaName, tableName string, fk schemalog.ForeignKey) *query {
	if fk.Name == "" || fk.Definition == "" {
		return nil
	}

	addQuery := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
		quotedTableName(schemaName, tableName),
		pglib.QuoteIdentifier(fk.Name),
		fk.Definition,
	)
	return &query{
		schema: schemaName,
		table:  tableName,
		sql:    addQuery,
		isDDL:  true,
	}
}
