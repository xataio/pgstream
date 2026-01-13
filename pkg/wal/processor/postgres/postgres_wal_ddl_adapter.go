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

const cycleOptionYes = "YES"

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

	return a.schemaDiffToQueries(schemaLog.SchemaName, diff)
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

	if diff.SchemaDropped {
		dropQuery := fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pglib.QuoteIdentifier(schemaName))
		return []*query{a.newDDLQuery(schemaName, "", dropQuery)}, nil
	}

	queries := []*query{
		a.createSchemaIfNotExists(schemaName),
	}

	sequenceQueries, dropSequenceQueries := a.buildSequenceQueries(schemaName, diff)
	mvQueries, dropMVQueries := a.buildMaterializedViewQueries(schemaName, diff)
	tableQueries, fkQueries := a.buildTableQueries(schemaName, diff)

	// materialized views are dropped first to avoid dependency issues when they
	// depend on tables/sequences that are being dropped
	queries = append(queries, dropMVQueries...)
	// create sequences first to avoid dependency issues when creating tables,
	// since the columns may depend on them
	queries = append(queries, sequenceQueries...)
	queries = append(queries, tableQueries...)

	queries = append(queries, mvQueries...)
	// drop sequences last to avoid dependency issues when dropping sequences
	// that are used by table columns
	queries = append(queries, dropSequenceQueries...)

	// append foreign key queries at the end to avoid dependency issues
	return append(queries, fkQueries...), nil
}

func (a *ddlAdapter) buildTableQueries(schemaName string, diff *schemalog.Diff) ([]*query, []*query) {
	queries := []*query{}
	for _, table := range diff.TablesRemoved {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTableName(schemaName, table.Name))
		queries = append(queries, a.newDDLQuery(schemaName, table.Name, dropQuery))
	}

	fkQueries := []*query{}
	for _, table := range diff.TablesAdded {
		queries = append(queries, a.buildCreateTableQuery(schemaName, table))
		queries = append(queries, a.buildCreateTableIndexQueries(schemaName, table)...)
		queries = append(queries, a.buildAddConstraintQueries(schemaName, table)...)
		fkQueries = append(fkQueries, a.buildAddForeignKeyQueries(schemaName, table)...)
	}

	for _, tableDiff := range diff.TablesChanged {
		alterQueries, alterFKQueries := a.buildAlterTableQueries(schemaName, tableDiff)
		queries = append(queries, alterQueries...)
		fkQueries = append(fkQueries, alterFKQueries...)
	}

	return queries, fkQueries
}

func (a *ddlAdapter) buildMaterializedViewQueries(schemaName string, diff *schemalog.Diff) ([]*query, []*query) {
	dropQueries := []*query{}
	for _, mv := range diff.MaterializedViewsRemoved {
		dropQuery := fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", pglib.QuoteQualifiedIdentifier(schemaName, mv.Name))
		dropQueries = append(dropQueries, a.newDDLQuery(schemaName, mv.Name, dropQuery))
	}

	queries := []*query{}
	for _, mv := range diff.MaterializedViewsAdded {
		createQuery := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s", pglib.QuoteQualifiedIdentifier(schemaName, mv.Name), mv.Definition)
		queries = append(queries, a.newDDLQuery(schemaName, mv.Name, createQuery))
		queries = append(queries, a.buildCreateIndexQueries(schemaName, mv.Name, mv.Indexes, nil)...)
	}

	for _, mv := range diff.MaterializedViewsChanged {
		alterQueries := a.buildAlterMaterializedViewQueries(schemaName, mv)
		queries = append(queries, alterQueries...)
	}
	return queries, dropQueries
}

func (a *ddlAdapter) buildSequenceQueries(schemaName string, diff *schemalog.Diff) ([]*query, []*query) {
	dropQueries := []*query{}
	for _, seq := range diff.SequencesRemoved {
		dropQuery := fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", pglib.QuoteQualifiedIdentifier(schemaName, seq.Name))
		dropQueries = append(dropQueries, a.newDDLQuery(schemaName, seq.Name, dropQuery))
	}

	queries := []*query{}
	for _, seq := range diff.SequencesAdded {
		createQuery := a.buildCreateSequenceQuery(schemaName, seq)
		queries = append(queries, a.newDDLQuery(schemaName, seq.Name, createQuery))
	}

	for _, seqDiff := range diff.SequencesChanged {
		alterQueries := a.buildAlterSequenceQueries(schemaName, seqDiff)
		queries = append(queries, alterQueries...)
	}

	return queries, dropQueries
}

func (a *ddlAdapter) buildCreateSequenceQuery(schemaName string, seq schemalog.Sequence) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", pglib.QuoteQualifiedIdentifier(schemaName, seq.Name)))

	// Add AS data_type clause
	if seq.DataType != nil && *seq.DataType != "" {
		parts = append(parts, fmt.Sprintf("AS %s", *seq.DataType))
	}

	// Add INCREMENT BY clause
	if seq.Increment != nil && *seq.Increment != "" {
		parts = append(parts, fmt.Sprintf("INCREMENT BY %s", *seq.Increment))
	}

	// Add MINVALUE/NO MINVALUE clause
	if seq.MinimumValue != nil && *seq.MinimumValue != "" {
		parts = append(parts, fmt.Sprintf("MINVALUE %s", *seq.MinimumValue))
	}

	// Add MAXVALUE/NO MAXVALUE clause
	if seq.MaximumValue != nil && *seq.MaximumValue != "" {
		parts = append(parts, fmt.Sprintf("MAXVALUE %s", *seq.MaximumValue))
	}

	// Add START WITH clause
	if seq.StartValue != nil && *seq.StartValue != "" {
		parts = append(parts, fmt.Sprintf("START WITH %s", *seq.StartValue))
	}

	// Add CYCLE/NO CYCLE clause
	if seq.CycleOption != nil && *seq.CycleOption != "" {
		if *seq.CycleOption == cycleOptionYes {
			parts = append(parts, "CYCLE")
		} else {
			parts = append(parts, "NO CYCLE")
		}
	}

	return strings.Join(parts, " ")
}

func (a *ddlAdapter) buildAlterSequenceQueries(schemaName string, seqDiff schemalog.SequenceDiff) []*query {
	queries := []*query{}
	qualifiedName := pglib.QuoteQualifiedIdentifier(schemaName, seqDiff.SequenceName)

	// Handle name change separately as it needs ALTER SEQUENCE ... RENAME TO
	if seqDiff.NameChange != nil {
		renameQuery := fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s RENAME TO %s",
			pglib.QuoteQualifiedIdentifier(schemaName, seqDiff.NameChange.Old),
			pglib.QuoteIdentifier(seqDiff.NameChange.New))
		queries = append(queries, a.newDDLQuery(schemaName, seqDiff.SequenceName, renameQuery))
		// Update qualified name for subsequent alterations
		qualifiedName = pglib.QuoteQualifiedIdentifier(schemaName, seqDiff.NameChange.New)
	}

	// Build ALTER SEQUENCE statement for other changes
	var alterParts []string

	if seqDiff.DataTypeChange != nil && seqDiff.DataTypeChange.New != nil && *seqDiff.DataTypeChange.New != "" {
		alterParts = append(alterParts, fmt.Sprintf("AS %s", *seqDiff.DataTypeChange.New))
	}

	if seqDiff.IncrementChange != nil && seqDiff.IncrementChange.New != nil && *seqDiff.IncrementChange.New != "" {
		alterParts = append(alterParts, fmt.Sprintf("INCREMENT BY %s", *seqDiff.IncrementChange.New))
	}

	if seqDiff.MinimumValueChange != nil && seqDiff.MinimumValueChange.New != nil && *seqDiff.MinimumValueChange.New != "" {
		alterParts = append(alterParts, fmt.Sprintf("MINVALUE %s", *seqDiff.MinimumValueChange.New))
	}

	if seqDiff.MaximumValueChange != nil && seqDiff.MaximumValueChange.New != nil && *seqDiff.MaximumValueChange.New != "" {
		alterParts = append(alterParts, fmt.Sprintf("MAXVALUE %s", *seqDiff.MaximumValueChange.New))
	}

	if seqDiff.StartValueChange != nil && seqDiff.StartValueChange.New != nil && *seqDiff.StartValueChange.New != "" {
		// For existing sequences, use RESTART WITH instead of START WITH
		alterParts = append(alterParts, fmt.Sprintf("RESTART WITH %s", *seqDiff.StartValueChange.New))
	}

	if seqDiff.CycleOptionChange != nil && seqDiff.CycleOptionChange.New != nil && *seqDiff.CycleOptionChange.New != "" {
		if *seqDiff.CycleOptionChange.New == cycleOptionYes {
			alterParts = append(alterParts, "CYCLE")
		} else {
			alterParts = append(alterParts, "NO CYCLE")
		}
	}

	if len(alterParts) > 0 {
		alterQuery := fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s %s", qualifiedName, strings.Join(alterParts, " "))
		queries = append(queries, a.newDDLQuery(schemaName, seqDiff.SequenceName, alterQuery))
	}

	return queries
}

func (a *ddlAdapter) buildCreateTableIndexQueries(schemaName string, table schemalog.Table) []*query {
	constraintIndexes := constraintBackedIndexNames(table.Constraints)
	return a.buildCreateIndexQueries(schemaName, table.Name, table.Indexes, constraintIndexes)
}

func (a *ddlAdapter) buildCreateIndexQueries(schemaName, objectName string, indexes []schemalog.Index, skip map[string]struct{}) []*query {
	queries := make([]*query, 0, len(indexes))
	for _, idx := range indexes {
		if idx.Definition == "" {
			continue
		}
		if _, skipIdx := skip[idx.Name]; skipIdx {
			continue
		}
		createQuery := ensureIndexHasIfNotExists(idx.Definition)
		queries = append(queries, a.newDDLQuery(schemaName, objectName, createQuery))
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

	switch {
	case column.Identity != "":
		switch column.Identity {
		case "a":
			colDefinition = fmt.Sprintf("%s GENERATED ALWAYS AS IDENTITY", colDefinition)
		case "d":
			colDefinition = fmt.Sprintf("%s GENERATED BY DEFAULT AS IDENTITY", colDefinition)
		}
	case column.GeneratedKind != "" && column.DefaultValue != nil:
		switch column.GeneratedKind {
		case "v":
			colDefinition = fmt.Sprintf("%s GENERATED ALWAYS AS (%s) VIRTUAL", colDefinition, *column.DefaultValue)
		case "s":
			colDefinition = fmt.Sprintf("%s GENERATED ALWAYS AS (%s) STORED", colDefinition, *column.DefaultValue)
		}
	default:
		// replicate default values (including those involving sequences) so that
		// sequence behavior is explicitly aligned between source and target.
		if column.DefaultValue != nil {
			colDefinition = fmt.Sprintf("%s DEFAULT %s", colDefinition, *column.DefaultValue)
		}
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

func (a *ddlAdapter) buildAlterMaterializedViewQueries(schemaName string, mvDiff schemalog.MaterializedViewsDiff) []*query {
	queries := []*query{}
	if mvDiff.NameChange != nil {
		alterQuery := fmt.Sprintf("ALTER MATERIALIZED VIEW IF EXISTS %s RENAME TO %s",
			pglib.QuoteQualifiedIdentifier(schemaName, mvDiff.NameChange.Old),
			pglib.QuoteIdentifier(mvDiff.NameChange.New),
		)
		queries = append(queries, a.newDDLQuery(schemaName, mvDiff.MaterializedViewName, alterQuery))
	}

	for _, idx := range mvDiff.IndexesAdded {
		if idx.Definition == "" {
			continue
		}
		createQuery := ensureIndexHasIfNotExists(idx.Definition)
		queries = append(queries, a.newDDLQuery(schemaName, mvDiff.MaterializedViewName, createQuery))
	}

	for _, idx := range mvDiff.IndexesRemoved {
		dropQuery := buildDropIndexQuery(schemaName, idx.Name)
		if dropQuery == "" {
			continue
		}
		queries = append(queries, a.newDDLQuery(schemaName, mvDiff.MaterializedViewName, dropQuery))
	}

	for _, definition := range mvDiff.IndexesChanged {
		if definition == "" {
			continue
		}
		queries = append(queries, a.newDDLQuery(schemaName, mvDiff.MaterializedViewName, definition))
	}

	return queries
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

	if columnDiff.GeneratedChange != nil {
		// Only support removing generated columns.
		// Adding generated column is not supported via ALTER TABLE in Postgres
		if !columnDiff.GeneratedChange.New && columnDiff.GeneratedChange.Old {
			alterQuery := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP EXPRESSION",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
			queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
		}
	}

	if columnDiff.IdentityChange != nil {
		alterQuery := ""
		switch {
		case columnDiff.IdentityChange.New == "":
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP IDENTITY",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		case columnDiff.IdentityChange.Old == "":
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED %s AS IDENTITY",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
				getIdentityKind(columnDiff.IdentityChange.New),
			)
		default:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET GENERATED %s",
				quotedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
				getIdentityKind(columnDiff.IdentityChange.New),
			)
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

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

func getIdentityKind(identity string) string {
	switch identity {
	case "a":
		return "ALWAYS"
	case "d":
		return "BY DEFAULT"
	default:
		return ""
	}
}
