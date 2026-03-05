// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"regexp"
	"strings"
)

type SchemaDiff struct {
	SchemaName    string
	SchemaDropped bool
	TablesAdded   []DDLObject
	TablesRemoved []DDLObject
	TablesChanged []TableDiff
}

type TableDiff struct {
	TableName             string
	TablePgstreamID       string
	TableNameChange       *ValueChange[string]
	TablePrimaryKeyChange *ValueChange[[]string]
	ColumnsAdded          []DDLColumn
	ColumnsRemoved        []DDLColumn
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
	GeneratedChange  *ValueChange[bool]
	IdentityChange   *ValueChange[string]
}

type ValueChange[T any] struct {
	Old, New T
}

func (d *SchemaDiff) IsEmpty() bool {
	return !d.SchemaDropped &&
		len(d.TablesAdded) == 0 &&
		len(d.TablesChanged) == 0 &&
		len(d.TablesRemoved) == 0
}

func (td *TableDiff) IsEmpty() bool {
	return len(td.ColumnsAdded) == 0 &&
		len(td.ColumnsRemoved) == 0 &&
		len(td.ColumnsChanged) == 0 &&
		td.TableNameChange == nil &&
		td.TablePrimaryKeyChange == nil
}

func DDLEventToSchemaDiff(ddlEvent *DDLEvent) (*SchemaDiff, error) {
	if ddlEvent == nil {
		return &SchemaDiff{}, nil
	}

	diff := &SchemaDiff{
		SchemaName: ddlEvent.SchemaName,
	}

	switch ddlEvent.CommandTag {
	case "DROP SCHEMA":
		diff.SchemaDropped = true
	case "DROP TABLE":
		diff.TablesRemoved = append(diff.TablesRemoved, ddlEvent.GetTableObjects()...)
	case "CREATE TABLE", "CREATE TABLE AS", "SELECT INTO":
		diff.TablesAdded = append(diff.TablesAdded, ddlEvent.GetTableObjects()...)
	case "ALTER TABLE":
		diff.TablesChanged = ddlEventToTableDiff(ddlEvent)
	}

	return diff, nil
}

func ddlEventToTableDiff(ddlEvent *DDLEvent) []TableDiff {
	// Parse the DDL command to determine what type of alteration was made
	ddlLower := strings.ToLower(ddlEvent.DDL)
	var tableDiffs []TableDiff

	for _, obj := range append(ddlEvent.GetTableObjects(), ddlEvent.GetTableColumnObjects()...) {
		tableDiff := TableDiff{
			TableName:       obj.GetTable(),
			TablePgstreamID: obj.PgstreamID,
		}

		switch {
		case strings.Contains(ddlLower, "add constraint") && strings.Contains(ddlLower, "primary key"):
			// Primary key was added
			if len(obj.PrimaryKeyColumns) > 0 {
				tableDiff.TablePrimaryKeyChange = &ValueChange[[]string]{
					Old: nil,
					New: obj.PrimaryKeyColumns,
				}
			}

		case strings.Contains(ddlLower, "rename to"):
			// Table was renamed
			// Pattern: "ALTER TABLE old_name RENAME TO new_name"
			if oldName := extractOldTableName(ddlEvent.DDL); oldName != "" {
				tableDiff.TableNameChange = &ValueChange[string]{
					Old: oldName,
					New: obj.GetTable(),
				}
			}

		case strings.Contains(ddlLower, "add column"):
			// Extract the column name from the DDL command
			// Pattern: "ALTER TABLE ... ADD COLUMN column_name ..."
			if colName := extractAddedColumnName(ddlEvent.DDL); colName != "" {
				if col, found := obj.GetColumnByName(colName); found {
					tableDiff.ColumnsAdded = append(tableDiff.ColumnsAdded, *col)
				}
			}

		case strings.Contains(ddlLower, "alter column"):
			// Extract the column name from the DDL command
			// Pattern: "ALTER TABLE ... ALTER COLUMN column_name ..."
			if colName := extractAlteredColumnName(ddlEvent.DDL); colName != "" {
				if col, found := obj.GetColumnByName(colName); found {
					// We don't know the old value, so we mark it as changed with nil old value
					tableDiff.ColumnsChanged = append(tableDiff.ColumnsChanged, ColumnDiff{
						ColumnName: col.Name,
						TypeChange: &ValueChange[string]{Old: "", New: col.Type},
					})
				}
			}

		case strings.Contains(ddlLower, "drop column"):
			// Extract the column name from the DDL command
			// Pattern: "ALTER TABLE ... DROP COLUMN column_name"
			if colName := extractDroppedColumnName(ddlEvent.DDL); colName != "" {
				tableDiff.ColumnsRemoved = append(tableDiff.ColumnsRemoved, DDLColumn{Name: colName})
			}

		case strings.Contains(ddlLower, "rename column"):
			// Pattern: "ALTER TABLE ... RENAME COLUMN old_name TO new_name"
			if oldName, newName := extractRenamedColumnNames(ddlEvent.DDL); oldName != "" && newName != "" {
				if col, found := obj.GetColumnByName(newName); found {
					tableDiff.ColumnsChanged = append(tableDiff.ColumnsChanged, ColumnDiff{
						ColumnName:       col.Name,
						ColumnPgstreamID: col.GetColumnPgstreamID(obj.PgstreamID),
						NameChange:       &ValueChange[string]{Old: oldName, New: newName},
					})
				}
			}

		default:
			// For other ALTER TABLE operations, we don't have enough information
			// to determine what changed without comparing with previous state
		}

		if !tableDiff.IsEmpty() {
			tableDiffs = append(tableDiffs, tableDiff)
		}
	}

	return tableDiffs
}

var addColumnRegex = regexp.MustCompile(`(?i)ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|(\S+))`)

// extractAddedColumnName extracts the column name from an "ADD COLUMN" DDL statement
func extractAddedColumnName(ddl string) string {
	// Pattern: ALTER TABLE ... ADD COLUMN column_name ...
	matches := addColumnRegex.FindStringSubmatch(ddl)
	if len(matches) > 1 {
		if matches[1] != "" {
			return matches[1] // quoted name
		}
		return matches[2] // unquoted name
	}
	return ""
}

var alterColumnRegex = regexp.MustCompile(`(?i)ALTER\s+COLUMN\s+(?:"([^"]+)"|(\S+))`)

// extractAlteredColumnName extracts the column name from an "ALTER COLUMN" DDL statement
func extractAlteredColumnName(ddl string) string {
	// Pattern: ALTER TABLE ... ALTER COLUMN column_name ...
	matches := alterColumnRegex.FindStringSubmatch(ddl)
	if len(matches) > 1 {
		if matches[1] != "" {
			return matches[1] // quoted name
		}
		return matches[2] // unquoted name
	}
	return ""
}

var dropColumnRegex = regexp.MustCompile(`(?i)DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(?:"([^"]+)"|(\S+))`)

// extractDroppedColumnName extracts the column name from a "DROP COLUMN" DDL statement
func extractDroppedColumnName(ddl string) string {
	// Pattern: ALTER TABLE ... DROP COLUMN column_name
	matches := dropColumnRegex.FindStringSubmatch(ddl)
	if len(matches) > 1 {
		if matches[1] != "" {
			return matches[1] // quoted name
		}
		return matches[2] // unquoted name
	}
	return ""
}

var renameColumnRegex = regexp.MustCompile(`(?i)RENAME\s+COLUMN\s+(?:"([^"]+)"|(\S+))\s+TO\s+(?:"([^"]+)"|(\S+))`)

// extractRenamedColumnNames extracts old and new column names from a "RENAME COLUMN" DDL statement
func extractRenamedColumnNames(ddl string) (oldName, newName string) {
	// Pattern: ALTER TABLE ... RENAME COLUMN old_name TO new_name
	matches := renameColumnRegex.FindStringSubmatch(ddl)
	if len(matches) > 1 {
		if matches[1] != "" {
			oldName = matches[1]
		} else {
			oldName = matches[2]
		}
		if matches[3] != "" {
			newName = matches[3]
		} else {
			newName = matches[4]
		}
	}
	return oldName, newName
}

var renameTableRegex = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(?:"([^"]+)"|(\S+))\s+RENAME\s+TO\s+(?:"([^"]+)"|(\S+))`)

// extractOldTableName extracts the old table name from a "RENAME TO" DDL statement
func extractOldTableName(ddl string) string {
	// Pattern: ALTER TABLE old_name RENAME TO new_name
	matches := renameTableRegex.FindStringSubmatch(ddl)
	if len(matches) > 1 {
		if matches[2] != "" {
			return matches[2] // quoted name
		}
		return matches[3] // unquoted name
	}
	return ""
}
