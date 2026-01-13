// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"slices"
	"strings"
)

type Diff struct {
	SchemaDropped            bool
	TablesRemoved            []Table
	TablesAdded              []Table
	TablesChanged            []TableDiff
	MaterializedViewsRemoved []MaterializedView
	MaterializedViewsAdded   []MaterializedView
	MaterializedViewsChanged []MaterializedViewsDiff
	SequencesRemoved         []Sequence
	SequencesAdded           []Sequence
	SequencesChanged         []SequenceDiff
}

type MaterializedViewsDiff struct {
	MaterializedViewName string
	NameChange           *ValueChange[string]
	IndexesAdded         []Index
	IndexesRemoved       []Index
	IndexesChanged       []string
}

type SequenceDiff struct {
	SequenceName       string
	NameChange         *ValueChange[string]
	DataTypeChange     *ValueChange[*string]
	IncrementChange    *ValueChange[*string]
	MinimumValueChange *ValueChange[*string]
	MaximumValueChange *ValueChange[*string]
	StartValueChange   *ValueChange[*string]
	CycleOptionChange  *ValueChange[*string]
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
	IndexesChanged        []string
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
	GeneratedChange  *ValueChange[bool]
	IdentityChange   *ValueChange[string]
}

type ValueChange[T any] struct {
	Old, New T
}

func (d *Diff) IsEmpty() bool {
	return !d.SchemaDropped &&
		len(d.TablesAdded) == 0 &&
		len(d.TablesChanged) == 0 &&
		len(d.TablesRemoved) == 0 &&
		len(d.MaterializedViewsAdded) == 0 &&
		len(d.MaterializedViewsRemoved) == 0 &&
		len(d.MaterializedViewsChanged) == 0 &&
		len(d.SequencesAdded) == 0 &&
		len(d.SequencesRemoved) == 0 &&
		len(d.SequencesChanged) == 0
}

func (mv *MaterializedViewsDiff) IsEmpty() bool {
	return len(mv.IndexesAdded) == 0 &&
		len(mv.IndexesRemoved) == 0 &&
		len(mv.IndexesChanged) == 0 &&
		mv.NameChange == nil
}

func (sd *SequenceDiff) IsEmpty() bool {
	return sd.NameChange == nil &&
		sd.DataTypeChange == nil &&
		sd.IncrementChange == nil &&
		sd.MinimumValueChange == nil &&
		sd.MaximumValueChange == nil &&
		sd.StartValueChange == nil &&
		sd.CycleOptionChange == nil
}

func (td *TableDiff) IsEmpty() bool {
	return len(td.ColumnsAdded) == 0 &&
		len(td.ColumnsRemoved) == 0 &&
		len(td.ColumnsChanged) == 0 &&
		len(td.IndexesAdded) == 0 &&
		len(td.IndexesRemoved) == 0 &&
		len(td.IndexesChanged) == 0 &&
		len(td.ConstraintsAdded) == 0 &&
		len(td.ConstraintsRemoved) == 0 &&
		len(td.ForeignKeysAdded) == 0 &&
		len(td.ForeignKeysRemoved) == 0 &&
		td.TableNameChange == nil &&
		td.TablePrimaryKeyChange == nil
}

func (cd *ColumnDiff) IsEmpty() bool {
	return cd.TypeChange == nil &&
		cd.NameChange == nil &&
		cd.DefaultChange == nil &&
		cd.NullChange == nil &&
		cd.UniqueChange == nil &&
		cd.GeneratedChange == nil &&
		cd.IdentityChange == nil
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

	// Schema dropped
	if !old.Schema.Dropped && new.Schema.Dropped {
		diff.SchemaDropped = true
		return diff
	}

	// Table changes

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

	// Materialized View changes

	newMVMap := getSchemaMaterializedViewMap(&new.Schema)
	for _, oldMV := range old.Schema.MaterializedViews {
		if _, found := newMVMap[oldMV.Oid]; !found {
			diff.MaterializedViewsRemoved = append(diff.MaterializedViewsRemoved, oldMV)
		}
	}

	oldMVMap := getSchemaMaterializedViewMap(&old.Schema)
	for oid, newMV := range newMVMap {
		oldMV, found := oldMVMap[oid]
		if !found {
			diff.MaterializedViewsAdded = append(diff.MaterializedViewsAdded, newMV)
			continue
		}

		// both schemas have the materialized view, check for changes
		mvDiff := computeMaterializedViewDiff(&oldMV, &newMV)
		if !mvDiff.IsEmpty() {
			diff.MaterializedViewsChanged = append(diff.MaterializedViewsChanged, *mvDiff)
		}
	}

	// Sequence changes

	newSeqMap := getSchemaSequenceMap(&new.Schema)
	for _, oldSeq := range old.Schema.Sequences {
		if _, found := newSeqMap[oldSeq.Oid]; !found {
			diff.SequencesRemoved = append(diff.SequencesRemoved, oldSeq)
		}
	}

	oldSeqMap := getSchemaSequenceMap(&old.Schema)
	for oid, newSeq := range newSeqMap {
		oldSeq, found := oldSeqMap[oid]
		if !found {
			diff.SequencesAdded = append(diff.SequencesAdded, newSeq)
			continue
		}

		// both schemas have the sequence, check for changes
		seqDiff := computeSequenceDiff(&oldSeq, &newSeq)
		if !seqDiff.IsEmpty() {
			diff.SequencesChanged = append(diff.SequencesChanged, *seqDiff)
		}
	}

	return diff
}

func computeMaterializedViewDiff(old, new *MaterializedView) *MaterializedViewsDiff {
	diff := &MaterializedViewsDiff{
		MaterializedViewName: new.Name,
	}

	if old.Name != new.Name {
		diff.NameChange = &ValueChange[string]{Old: old.Name, New: new.Name}
	}

	newIndexMap := getMaterializedViewIndexMap(new)
	for _, oldIdx := range old.Indexes {
		newIdx, found := newIndexMap[oldIdx.Name]
		if !found {
			diff.IndexesRemoved = append(diff.IndexesRemoved, oldIdx)
			continue
		}

		if !oldIdx.IsEqual(&newIdx) {
			if isAlterIndexDefinition(newIdx.Definition) {
				diff.IndexesChanged = append(diff.IndexesChanged, newIdx.Definition)
				continue
			}

			diff.IndexesRemoved = append(diff.IndexesRemoved, oldIdx)
			diff.IndexesAdded = append(diff.IndexesAdded, newIdx)
		}
	}

	oldIndexMap := getMaterializedViewIndexMap(old)
	for name, newIdx := range newIndexMap {
		if _, found := oldIndexMap[name]; !found {
			diff.IndexesAdded = append(diff.IndexesAdded, newIdx)
		}
	}

	return diff
}

func computeSequenceDiff(old, new *Sequence) *SequenceDiff {
	diff := &SequenceDiff{
		SequenceName: new.Name,
	}

	if old.Name != new.Name {
		diff.NameChange = &ValueChange[string]{Old: old.Name, New: new.Name}
	}

	if !isEqualStrPtr(old.DataType, new.DataType) {
		diff.DataTypeChange = &ValueChange[*string]{Old: old.DataType, New: new.DataType}
	}

	if !isEqualStrPtr(old.Increment, new.Increment) {
		diff.IncrementChange = &ValueChange[*string]{Old: old.Increment, New: new.Increment}
	}

	if !isEqualStrPtr(old.MinimumValue, new.MinimumValue) {
		diff.MinimumValueChange = &ValueChange[*string]{Old: old.MinimumValue, New: new.MinimumValue}
	}

	if !isEqualStrPtr(old.MaximumValue, new.MaximumValue) {
		diff.MaximumValueChange = &ValueChange[*string]{Old: old.MaximumValue, New: new.MaximumValue}
	}

	if !isEqualStrPtr(old.StartValue, new.StartValue) {
		diff.StartValueChange = &ValueChange[*string]{Old: old.StartValue, New: new.StartValue}
	}

	if !isEqualStrPtr(old.CycleOption, new.CycleOption) {
		diff.CycleOptionChange = &ValueChange[*string]{Old: old.CycleOption, New: new.CycleOption}
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
			if isAlterIndexDefinition(newIdx.Definition) {
				diff.IndexesChanged = append(diff.IndexesChanged, newIdx.Definition)
				continue
			}

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

	// do not compute default changes for generated columns
	if !old.IsGenerated() && !new.IsGenerated() &&
		!isEqualStrPtr(old.DefaultValue, new.DefaultValue) {
		diff.DefaultChange = &ValueChange[*string]{Old: old.DefaultValue, New: new.DefaultValue}
	}
	if old.Unique != new.Unique {
		diff.UniqueChange = &ValueChange[bool]{Old: old.Unique, New: new.Unique}
	}

	if old.Nullable != new.Nullable {
		diff.NullChange = &ValueChange[bool]{Old: old.Nullable, New: new.Nullable}
	}

	if old.Generated != new.Generated {
		diff.GeneratedChange = &ValueChange[bool]{Old: old.Generated, New: new.Generated}
	}

	if old.Identity != new.Identity {
		diff.IdentityChange = &ValueChange[string]{Old: old.Identity, New: new.Identity}
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

func getSchemaMaterializedViewMap(s *Schema) map[string]MaterializedView {
	mvMap := make(map[string]MaterializedView, len(s.MaterializedViews))
	for _, mv := range s.MaterializedViews {
		mvMap[mv.Oid] = mv
	}
	return mvMap
}

func getSchemaSequenceMap(s *Schema) map[string]Sequence {
	seqMap := make(map[string]Sequence, len(s.Sequences))
	for _, seq := range s.Sequences {
		seqMap[seq.Oid] = seq
	}
	return seqMap
}

func getMaterializedViewIndexMap(mv *MaterializedView) map[string]Index {
	indexMap := make(map[string]Index, len(mv.Indexes))
	for _, i := range mv.Indexes {
		indexMap[i.Name] = i
	}
	return indexMap
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

func isAlterIndexDefinition(definition string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(definition)), "ALTER INDEX")
}
