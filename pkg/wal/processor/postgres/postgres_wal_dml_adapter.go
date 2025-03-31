// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgstream/pkg/wal"
)

type onConflictAction uint

const (
	onConflictError onConflictAction = iota
	onConflictUpdate
	onConflictDoNothing
)

var errUnsupportedOnConflictAction = errors.New("unsupported on conflict action")

type dmlAdapter struct {
	onConflictAction onConflictAction
}

func newDMLAdapter(action string) (*dmlAdapter, error) {
	oca, err := parseOnConflictAction(action)
	if err != nil {
		return nil, err
	}
	return &dmlAdapter{
		onConflictAction: oca,
	}, nil
}

func (a *dmlAdapter) walDataToQuery(d *wal.Data) *query {
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
		return &query{}
	}
}

func (a *dmlAdapter) buildTruncateQuery(d *wal.Data) *query {
	return &query{
		sql: fmt.Sprintf("TRUNCATE %s", quotedTableName(d.Schema, d.Table)),
	}
}

func (a *dmlAdapter) buildDeleteQuery(d *wal.Data) *query {
	primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Identity)
	whereQuery, whereValues := a.buildWhereQuery(primaryKeyCols, 0)
	return &query{
		sql:  fmt.Sprintf("DELETE FROM %s %s", quotedTableName(d.Schema, d.Table), whereQuery),
		args: whereValues,
	}
}

func (a *dmlAdapter) buildInsertQuery(d *wal.Data) *query {
	names := make([]string, 0, len(d.Columns))
	values := make([]any, 0, len(d.Columns))
	placeholders := make([]string, 0, len(d.Columns))
	for i, col := range d.Columns {
		names = append(names, col.Name)
		values = append(values, col.Value)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	return &query{
		sql: fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)%s",
			quotedTableName(d.Schema, d.Table), strings.Join(names, ", "),
			strings.Join(placeholders, ", "),
			a.buildOnConflictQuery(d)),
		args: values,
	}
}

func (a *dmlAdapter) buildUpdateQuery(d *wal.Data) *query {
	setQuery, setValues := a.buildSetQuery(d.Columns)
	primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Columns)
	whereQuery, whereValues := a.buildWhereQuery(primaryKeyCols, len(d.Columns))
	return &query{
		sql:  fmt.Sprintf("UPDATE %s %s %s", quotedTableName(d.Schema, d.Table), setQuery, whereQuery),
		args: append(setValues, whereValues...),
	}
}

func (a *dmlAdapter) buildWhereQuery(cols []wal.Column, placeholderOffset int) (string, []any) {
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

func (a *dmlAdapter) buildSetQuery(cols []wal.Column) (string, []any) {
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

func (a *dmlAdapter) buildOnConflictQuery(d *wal.Data) string {
	switch a.onConflictAction {
	case onConflictUpdate:
		// on conflict do update requires a conflict target. If there are no
		// primary keys to use for the conflict target, default to error
		// behaviour
		primaryKeyCols := a.extractPrimaryKeyColumnNames(d.Metadata.InternalColIDs, d.Columns)
		if len(primaryKeyCols) == 0 {
			return ""
		}

		cols := make([]string, 0, len(d.Columns))
		for _, col := range d.Columns {
			cols = append(cols, fmt.Sprintf("%[1]s = EXCLUDED.%[1]s", pq.QuoteIdentifier(col.Name)))
		}
		return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(primaryKeyCols, ","), strings.Join(cols, ", "))
	case onConflictDoNothing:
		return " ON CONFLICT DO NOTHING"
	default:
		return ""
	}
}

func (a *dmlAdapter) extractPrimaryKeyColumns(colIDs []string, cols []wal.Column) []wal.Column {
	primaryKeyColumns := make([]wal.Column, 0, len(colIDs))
	for _, col := range cols {
		if !slices.Contains(colIDs, col.ID) {
			continue
		}
		primaryKeyColumns = append(primaryKeyColumns, col)
	}

	return primaryKeyColumns
}

func (a *dmlAdapter) extractPrimaryKeyColumnNames(colIDs []string, cols []wal.Column) []string {
	primaryKeyCols := a.extractPrimaryKeyColumns(colIDs, cols)
	if len(primaryKeyCols) == 0 {
		return []string{}
	}
	colNames := []string{}
	for _, col := range primaryKeyCols {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

func quotedTableName(schemaName, tableName string) string {
	return fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
}

func parseOnConflictAction(action string) (onConflictAction, error) {
	switch action {
	case "", "error":
		return onConflictError, nil
	case "update":
		return onConflictUpdate, nil
	case "nothing":
		return onConflictDoNothing, nil
	default:
		return 0, errUnsupportedOnConflictAction
	}
}
