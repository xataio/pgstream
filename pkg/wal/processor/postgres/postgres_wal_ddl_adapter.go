// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type ddlAdapter struct {
	schemalogQuerier schemalogQuerier
	schemaDiffer     schemaDiffer
	logEntryAdapter  logEntryAdapter
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
		logEntryAdapter:  processor.WalDataToLogEntry,
	}
}

func (a *ddlAdapter) walDataToQueries(ctx context.Context, d *wal.Data) ([]*query, error) {
	newSchemaLog, err := a.logEntryAdapter(d)
	if err != nil {
		return nil, err
	}

	previousSchemaLog, err := a.schemalogQuerier.Fetch(ctx, newSchemaLog.SchemaName, int(newSchemaLog.Version)-1)
	if err != nil && !errors.Is(err, schemalog.ErrNoRows) {
		return nil, fmt.Errorf("fetching existing schema log entry: %w", err)
	}

	diff := a.schemaDiffer(previousSchemaLog, newSchemaLog)

	return a.schemaDiffToQueries(newSchemaLog.SchemaName, diff)
}

func (a *ddlAdapter) schemaDiffToQueries(schemaName string, diff *schemalog.Diff) ([]*query, error) {
	if diff.IsEmpty() {
		return []*query{}, nil
	}

	queries := []*query{}
	for _, table := range diff.TablesRemoved {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTableName(schemaName, table.Name))
		queries = append(queries, a.newDDLQuery(dropQuery))
	}

	for _, table := range diff.TablesAdded {
		queries = append(queries, a.buildCreateTableQuery(schemaName, table))
	}

	for _, tableDiff := range diff.TablesChanged {
		queries = append(queries, a.buildAlterTableQueries(schemaName, tableDiff)...)
	}

	return queries, nil
}

func (a *ddlAdapter) buildCreateTableQuery(schemaName string, table schemalog.Table) *query {
	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", quotedTableName(schemaName, table.Name))
	uniqueConstraints := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		createQuery = fmt.Sprintf("%s %s,\n", createQuery, a.buildColumnDefinition(&col))
		// if there's a unique constraint associated to the column, and it's not
		// the primary key, explicitly add it
		if uniqueConstraint := a.buildUniqueConstraint(col); uniqueConstraint != "" && !slices.Contains(table.PrimaryKeyColumns, col.Name) {
			uniqueConstraints = append(uniqueConstraints, uniqueConstraint)
		}
	}

	for _, constraint := range uniqueConstraints {
		createQuery = fmt.Sprintf("%s %s,\n", createQuery, constraint)
	}

	if len(table.PrimaryKeyColumns) > 0 {
		createQuery = fmt.Sprintf("%s PRIMARY KEY (%s)\n", createQuery, strings.Join(table.PrimaryKeyColumns, ","))
	}

	createQuery = fmt.Sprintf("%s)", createQuery)

	return a.newDDLQuery(createQuery)
}

func (a *ddlAdapter) buildColumnDefinition(column *schemalog.Column) string {
	colDefinition := fmt.Sprintf("%s %s", column.Name, column.DataType)
	if !column.Nullable {
		colDefinition = fmt.Sprintf("%s NOT NULL", colDefinition)
	}
	// do not set default values with sequences since they will differ between
	// source/target. Keep source database as source of truth.
	if column.DefaultValue != nil && !strings.Contains(*column.DefaultValue, "seq") {
		colDefinition = fmt.Sprintf("%s DEFAULT %s", colDefinition, *column.DefaultValue)
	}

	return colDefinition
}

func (a *ddlAdapter) buildUniqueConstraint(column schemalog.Column) string {
	if column.Unique {
		return fmt.Sprintf("UNIQUE (%s)", column.Name)
	}
	return ""
}

func (a *ddlAdapter) buildAlterTableQueries(schemaName string, tableDiff schemalog.TableDiff) []*query {
	if tableDiff.IsEmpty() {
		return []*query{}
	}

	queries := []*query{}
	if tableDiff.TableNameChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			quotedTableName(schemaName, tableDiff.TableNameChange.Old),
			tableDiff.TableNameChange.New,
		)
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	for _, col := range tableDiff.ColumnsRemoved {
		alterQuery := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quotedTableName(schemaName, tableDiff.TableName), col.Name)
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	for _, col := range tableDiff.ColumnsAdded {
		alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quotedTableName(schemaName, tableDiff.TableName), a.buildColumnDefinition(&col))
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	for _, colDiff := range tableDiff.ColumnsChanged {
		alterQueries := a.buildAlterColumnQueries(schemaName, tableDiff.TableName, &colDiff)
		queries = append(queries, alterQueries...)
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
			columnDiff.NameChange.Old,
			columnDiff.NameChange.New,
		)
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	if columnDiff.TypeChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
			quotedTableName(schemaName, tableName),
			columnDiff.ColumnName,
			columnDiff.TypeChange.New,
		)
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	if columnDiff.NullChange != nil {
		alterQuery := ""
		switch {
		// from not nullable to nullable
		case columnDiff.NullChange.New:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
				quotedTableName(schemaName, tableName),
				columnDiff.ColumnName,
			)
		default:
			// from nullable to not nullable
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
				quotedTableName(schemaName, tableName),
				columnDiff.ColumnName,
			)
		}
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	if columnDiff.DefaultChange != nil {
		alterQuery := ""
		switch columnDiff.DefaultChange.New {
		// removing the default
		case nil:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
				quotedTableName(schemaName, tableName),
				columnDiff.ColumnName,
			)
		default:
			// do not set default values with sequences since they will differ between
			// source/target. Keep source database as source of truth.
			if !strings.Contains(*columnDiff.DefaultChange.New, "seq") {
				alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
					quotedTableName(schemaName, tableName),
					columnDiff.ColumnName,
					*columnDiff.DefaultChange.New,
				)
			}
		}
		queries = append(queries, a.newDDLQuery(alterQuery))
	}

	// TODO: add support for unique constraint changes

	return queries
}

func (a *ddlAdapter) newDDLQuery(sql string) *query {
	return &query{
		sql:   sql,
		isDDL: true,
	}
}
