// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type walAdapter interface {
	walEventToQuery(e *wal.Event) (*query, error)
}

type adapter struct{}

func (a *adapter) walEventToQuery(e *wal.Event) (*query, error) {
	// TODO: support DDL parsing
	if e.Data == nil || processor.IsSchemaLogEvent(e.Data) {
		return &query{}, nil
	}

	// DML events
	switch e.Data.Action {
	case "I", "U", "D", "T":
		return a.walDataToQuery(e.Data), nil
	default:
		// unsupported actions
		return &query{}, nil
	}
}

func (a *adapter) walDataToQuery(d *wal.Data) *query {
	switch d.Action {
	case "T":
		return a.buildTruncateQuery(d)
	case "D":
		return a.buildDeleteQuery(d)
	case "I":
		return a.buildInsertQuery(d)
	case "U":
		return a.buildUpdateQuery(d)
	default:
		return nil
	}
}

func (a *adapter) buildTruncateQuery(d *wal.Data) *query {
	return &query{
		sql: fmt.Sprintf("TRUNCATE %s", quotedTableName(d.Schema, d.Table)),
	}
}

func (a *adapter) buildDeleteQuery(d *wal.Data) *query {
	primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Identity)
	whereQuery, whereValues := a.buildWhereQuery(primaryKeyCols, 0)
	return &query{
		sql:  fmt.Sprintf("DELETE FROM %s %s", quotedTableName(d.Schema, d.Table), whereQuery),
		args: whereValues,
	}
}

func (a *adapter) buildInsertQuery(d *wal.Data) *query {
	names := make([]string, 0, len(d.Columns))
	values := make([]any, 0, len(d.Columns))
	placeholders := make([]string, 0, len(d.Columns))
	for i, col := range d.Columns {
		names = append(names, col.Name)
		values = append(values, col.Value)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	return &query{
		sql: fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) %s",
			quotedTableName(d.Schema, d.Table), strings.Join(names, ", "),
			strings.Join(placeholders, ", "),
			a.buildOnConflictQuery(d)),
		args: values,
	}
}

func (a *adapter) buildUpdateQuery(d *wal.Data) *query {
	setQuery, setValues := a.buildSetQuery(d.Columns)
	primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Columns)
	whereQuery, whereValues := a.buildWhereQuery(primaryKeyCols, len(d.Columns))
	return &query{
		sql:  fmt.Sprintf("UPDATE %s %s %s", quotedTableName(d.Schema, d.Table), setQuery, whereQuery),
		args: append(setValues, whereValues...),
	}
}

func (a *adapter) buildWhereQuery(cols []wal.Column, placeholderOffset int) (string, []any) {
	whereQuery := "WHERE"
	whereValues := make([]any, 0, len(cols))
	for i, c := range cols {
		if i != 0 {
			whereQuery = fmt.Sprintf("%s AND", whereQuery)
		}
		whereQuery = fmt.Sprintf("%s %s = $%d", whereQuery, c.Name, i+placeholderOffset+1)
		whereValues = append(whereValues, c.Value)
	}
	return whereQuery, whereValues
}

func (a *adapter) buildSetQuery(cols []wal.Column) (string, []any) {
	setQuery := "SET"
	setValues := make([]any, 0, len(cols))
	for i, c := range cols {
		if i != 0 {
			setQuery = fmt.Sprintf("%s,", setQuery)
		}
		setQuery = fmt.Sprintf("%s %s = $%d", setQuery, c.Name, i+1)
		setValues = append(setValues, c.Value)
	}
	return setQuery, setValues
}

func (a *adapter) buildOnConflictQuery(d *wal.Data) string {
	primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Columns)
	conflictCols := []string{}
	for _, col := range primaryKeyCols {
		conflictCols = append(conflictCols, col.Name)
	}
	if len(conflictCols) == 0 {
		return ""
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", strings.Join(conflictCols, ", "))
}

func (a *adapter) extractPrimaryKeyColumns(colIDs []string, cols []wal.Column) []wal.Column {
	primaryKeyColumns := make([]wal.Column, 0, len(colIDs))
	for _, col := range cols {
		if !slices.Contains(colIDs, col.ID) {
			continue
		}
		primaryKeyColumns = append(primaryKeyColumns, col)
	}

	return primaryKeyColumns
}

func quotedTableName(schemaName, tableName string) string {
	return fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
}
