// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"slices"
)

type Diff struct {
	TablesRemoved []Table
	TablesAdded   []Table
	TablesChanged []TableDiff
}

type TableDiff struct {
	TableName             string
	TablePgstreamID       string
	TableNameChange       *ValueChange[string]
	TablePrimaryKeyChange *ValueChange[[]string]
	ColumnsAdded          []Column
	ColumnsRemoved        []Column
	ColumnsChanged        []ColumnDiff
}

type ColumnDiff struct {
	ColumnName       string
	ColumnPgstreamID string
	NameChange       *ValueChange[string]
	TypeChange       *ValueChange[string]
	UniqueChange     *ValueChange[bool]
	NullChange       *ValueChange[bool]
	DefaultChange    *ValueChange[*string]
}

type ValueChange[T any] struct {
	Old, New T
}

func (d *Diff) IsEmpty() bool {
	return len(d.TablesAdded) == 0 && len(d.TablesChanged) == 0 && len(d.TablesRemoved) == 0
}

func (td *TableDiff) IsEmpty() bool {
	return len(td.ColumnsAdded) == 0 && len(td.ColumnsRemoved) == 0 && len(td.ColumnsChanged) == 0 && td.TableNameChange == nil && td.TablePrimaryKeyChange == nil
}

func (cd *ColumnDiff) IsEmpty() bool {
	return cd.TypeChange == nil && cd.NameChange == nil && cd.DefaultChange == nil && cd.NullChange == nil && cd.UniqueChange == nil
}

func ComputeSchemaDiff(old, new *LogEntry) *Diff {
	switch {
	case old == nil && new == nil:
		return &Diff{}
	case old == nil:
		old = &LogEntry{}
	case new == nil:
		new = &LogEntry{}
	}

	diff := &Diff{}
	newTableMap := getSchemaTableMap(&new.Schema)
	// if a table ID exists in the old schema, but not in the new, remove the table
	for _, oldTable := range old.Schema.Tables {
		if _, found := newTableMap[oldTable.PgstreamID]; !found {
			diff.TablesRemoved = append(diff.TablesRemoved, oldTable)
		}
	}

	oldTableMap := getSchemaTableMap(&old.Schema)
	for id, newTable := range newTableMap {
		oldTable, found := oldTableMap[id]
		// if the table is not on the old schema, add it
		if !found {
			diff.TablesAdded = append(diff.TablesAdded, newTable)
			continue
		}

		// both schemas have the table, check for changes
		tableDiff := computeTableDiff(&oldTable, &newTable)
		if !tableDiff.IsEmpty() {
			diff.TablesChanged = append(diff.TablesChanged, *tableDiff)
		}
	}

	return diff
}

func computeTableDiff(old, new *Table) *TableDiff {
	diff := &TableDiff{
		TableName:       new.Name,
		TablePgstreamID: new.PgstreamID,
	}

	if old.Name != new.Name {
		diff.TableNameChange = &ValueChange[string]{Old: old.Name, New: new.Name}
	}

	if !slices.Equal(old.PrimaryKeyColumns, new.PrimaryKeyColumns) {
		diff.TablePrimaryKeyChange = &ValueChange[[]string]{Old: old.PrimaryKeyColumns, New: new.PrimaryKeyColumns}
	}

	newColumnMap := getTableColumnMap(new)
	// if a column ID exists in the old table, but not in the new, remove the column
	for _, col := range old.Columns {
		if _, found := newColumnMap[col.PgstreamID]; !found {
			diff.ColumnsRemoved = append(diff.ColumnsRemoved, col)
		}
	}

	oldColumnMap := getTableColumnMap(old)
	for id, newCol := range newColumnMap {
		oldCol, found := oldColumnMap[id]
		if !found {
			diff.ColumnsAdded = append(diff.ColumnsAdded, newCol)
			continue
		}

		// both tables have the column, check for changes
		colDiff := computeColumnDiff(&oldCol, &newCol)
		if !colDiff.IsEmpty() {
			diff.ColumnsChanged = append(diff.ColumnsChanged, *colDiff)
		}
	}

	return diff
}

func computeColumnDiff(old, new *Column) *ColumnDiff {
	diff := &ColumnDiff{
		ColumnName:       new.Name,
		ColumnPgstreamID: new.PgstreamID,
	}

	if old.DataType != new.DataType {
		diff.TypeChange = &ValueChange[string]{Old: old.DataType, New: new.DataType}
	}
	if old.Name != new.Name {
		diff.NameChange = &ValueChange[string]{Old: old.Name, New: new.Name}
	}

	if (old.DefaultValue != nil && new.DefaultValue == nil) ||
		(old.DefaultValue == nil && new.DefaultValue != nil) ||
		(old.DefaultValue != nil && new.DefaultValue != nil && *old.DefaultValue != *new.DefaultValue) {
		diff.DefaultChange = &ValueChange[*string]{Old: old.DefaultValue, New: new.DefaultValue}
	}
	if old.Unique != new.Unique {
		diff.UniqueChange = &ValueChange[bool]{Old: old.Unique, New: new.Unique}
	}

	if old.Nullable != new.Nullable {
		diff.NullChange = &ValueChange[bool]{Old: old.Nullable, New: new.Nullable}
	}

	return diff
}

func getSchemaTableMap(s *Schema) map[string]Table {
	tableMap := make(map[string]Table, len(s.Tables))
	for _, t := range s.Tables {
		tableMap[t.PgstreamID] = t
	}
	return tableMap
}

func getTableColumnMap(t *Table) map[string]Column {
	columnMap := make(map[string]Column, len(t.Columns))
	for _, c := range t.Columns {
		columnMap[c.PgstreamID] = c
	}
	return columnMap
}
