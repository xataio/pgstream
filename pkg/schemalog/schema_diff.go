// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"slices"
	"strings"
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
	IndexesAdded          []Index
	IndexesRemoved        []Index
	IndexesRenamed        []IndexRename
	ConstraintsAdded      []Constraint
	ConstraintsRemoved    []Constraint
	ForeignKeysAdded      []ForeignKey
	ForeignKeysRemoved    []ForeignKey
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
	return len(td.ColumnsAdded) == 0 &&
		len(td.ColumnsRemoved) == 0 &&
		len(td.ColumnsChanged) == 0 &&
		len(td.IndexesAdded) == 0 &&
		len(td.IndexesRemoved) == 0 &&
		len(td.IndexesRenamed) == 0 &&
		len(td.ConstraintsAdded) == 0 &&
		len(td.ConstraintsRemoved) == 0 &&
		len(td.ForeignKeysAdded) == 0 &&
		len(td.ForeignKeysRemoved) == 0 &&
		td.TableNameChange == nil &&
		td.TablePrimaryKeyChange == nil
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

	newIndexMap := getTableIndexMap(new)
	for _, oldIdx := range old.Indexes {
		newIdx, found := newIndexMap[oldIdx.Name]
		if !found {
			diff.IndexesRemoved = append(diff.IndexesRemoved, oldIdx)
			continue
		}

		if !oldIdx.IsEqual(&newIdx) {
			diff.IndexesRemoved = append(diff.IndexesRemoved, oldIdx)
			diff.IndexesAdded = append(diff.IndexesAdded, newIdx)
		}
	}

	oldIndexMap := getTableIndexMap(old)
	for name, newIdx := range newIndexMap {
		if _, found := oldIndexMap[name]; !found {
			diff.IndexesAdded = append(diff.IndexesAdded, newIdx)
		}
	}

	matchRenamedIndexes(diff)

	newConstraintMap := getTableConstraintMap(new)
	for _, oldConstraint := range old.Constraints {
		newConstraint, found := newConstraintMap[oldConstraint.Name]
		if !found {
			diff.ConstraintsRemoved = append(diff.ConstraintsRemoved, oldConstraint)
			continue
		}

		if !oldConstraint.IsEqual(&newConstraint) {
			diff.ConstraintsRemoved = append(diff.ConstraintsRemoved, oldConstraint)
			diff.ConstraintsAdded = append(diff.ConstraintsAdded, newConstraint)
		}
	}

	oldConstraintMap := getTableConstraintMap(old)
	for name, newConstraint := range newConstraintMap {
		if _, found := oldConstraintMap[name]; !found {
			diff.ConstraintsAdded = append(diff.ConstraintsAdded, newConstraint)
		}
	}

	newForeignKeyMap := getTableForeignKeyMap(new)
	for _, oldForeignKey := range old.ForeignKeys {
		newForeignKey, found := newForeignKeyMap[oldForeignKey.Name]
		if !found {
			diff.ForeignKeysRemoved = append(diff.ForeignKeysRemoved, oldForeignKey)
			continue
		}

		if !oldForeignKey.IsEqual(&newForeignKey) {
			diff.ForeignKeysRemoved = append(diff.ForeignKeysRemoved, oldForeignKey)
			diff.ForeignKeysAdded = append(diff.ForeignKeysAdded, newForeignKey)
		}
	}

	oldForeignKeyMap := getTableForeignKeyMap(old)
	for name, newForeignKey := range newForeignKeyMap {
		if _, found := oldForeignKeyMap[name]; !found {
			diff.ForeignKeysAdded = append(diff.ForeignKeysAdded, newForeignKey)
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

func getTableIndexMap(t *Table) map[string]Index {
	indexMap := make(map[string]Index, len(t.Indexes))
	for _, i := range t.Indexes {
		indexMap[i.Name] = i
	}
	return indexMap
}

func getTableConstraintMap(t *Table) map[string]Constraint {
	constraintMap := make(map[string]Constraint, len(t.Constraints))
	for _, c := range t.Constraints {
		constraintMap[c.Name] = c
	}
	return constraintMap
}

func getTableForeignKeyMap(t *Table) map[string]ForeignKey {
	foreignKeyMap := make(map[string]ForeignKey, len(t.ForeignKeys))
	for _, fk := range t.ForeignKeys {
		foreignKeyMap[fk.Name] = fk
	}
	return foreignKeyMap
}

func matchRenamedIndexes(diff *TableDiff) {
	if len(diff.IndexesRemoved) == 0 || len(diff.IndexesAdded) == 0 {
		return
	}

	canonical := func(def string) string {
		fields := strings.Fields(def)
		if len(fields) < 4 {
			return def
		}
		upperPrefix := strings.ToUpper(fields[0])
		if upperPrefix != "CREATE" {
			return def
		}
		// find position of "INDEX"
		indexPos := -1
		for i, f := range fields {
			if strings.ToUpper(f) == "INDEX" {
				indexPos = i
				break
			}
		}
		if indexPos == -1 || indexPos+1 >= len(fields) {
			return def
		}
		// rebuild prefix without the next token (index name)
		newFields := append([]string{}, fields[:indexPos+1]...)
		canonicalPrefix := strings.Join(newFields, " ")
		// find the substring up to index name
		prefix := strings.Join(fields[:indexPos+2], " ")
		idx := strings.Index(def, prefix)
		if idx == -1 {
			return def
		}
		suffix := def[idx+len(prefix):]
		return canonicalPrefix + suffix
	}

	added := diff.IndexesAdded
	removed := diff.IndexesRemoved
	usedAdded := make([]bool, len(added))
	remainingRemoved := make([]Index, 0, len(removed))
	renamed := make([]IndexRename, 0)

	for _, oldIdx := range removed {
		oldCanonical := canonical(oldIdx.Definition)
		matched := false
		for j, newIdx := range added {
			if usedAdded[j] {
				continue
			}
			if canonical(newIdx.Definition) == oldCanonical {
				renamed = append(renamed, IndexRename{
					Old: oldIdx,
					New: newIdx,
				})
				usedAdded[j] = true
				matched = true
				break
			}
		}
		if !matched {
			remainingRemoved = append(remainingRemoved, oldIdx)
		}
	}

	remainingAdded := make([]Index, 0, len(added))
	for j, idx := range added {
		if !usedAdded[j] {
			remainingAdded = append(remainingAdded, idx)
		}
	}

	if len(remainingRemoved) == 0 {
		diff.IndexesRemoved = nil
	} else {
		diff.IndexesRemoved = remainingRemoved
	}
	if len(remainingAdded) == 0 {
		diff.IndexesAdded = nil
	} else {
		diff.IndexesAdded = remainingAdded
	}

	if len(renamed) > 0 {
		if len(diff.ConstraintsRemoved) > 0 {
			filtered := diff.ConstraintsRemoved[:0]
			renameMap := make(map[string]struct{}, len(renamed))
			for _, r := range renamed {
				renameMap[r.Old.Name] = struct{}{}
			}
			for _, constraint := range diff.ConstraintsRemoved {
				if strings.EqualFold(constraint.Type, "UNIQUE") {
					if _, ok := renameMap[constraint.Name]; ok {
						continue
					}
				}
				filtered = append(filtered, constraint)
			}
			if len(filtered) == 0 {
				diff.ConstraintsRemoved = nil
			} else {
				diff.ConstraintsRemoved = filtered
			}
		}

		if len(diff.ConstraintsAdded) > 0 {
			filtered := diff.ConstraintsAdded[:0]
			renameMap := make(map[string]struct{}, len(renamed))
			for _, r := range renamed {
				renameMap[r.New.Name] = struct{}{}
			}
			for _, constraint := range diff.ConstraintsAdded {
				if strings.EqualFold(constraint.Type, "UNIQUE") {
					if _, ok := renameMap[constraint.Name]; ok {
						continue
					}
				}
				filtered = append(filtered, constraint)
			}
			if len(filtered) == 0 {
				diff.ConstraintsAdded = nil
			} else {
				diff.ConstraintsAdded = filtered
			}
		}
	}

	diff.IndexesRenamed = append(diff.IndexesRenamed, renamed...)
}

type IndexRename struct {
	Old Index
	New Index
}
