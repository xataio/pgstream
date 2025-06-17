// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/wal"
)

type onConflictAction uint

const (
	onConflictError onConflictAction = iota
	onConflictUpdate
	onConflictDoNothing
)

var (
	errUnsupportedOnConflictAction = errors.New("unsupported on conflict action")
	errUnableToBuildQuery          = errors.New("unable to build query, no primary keys of previous values available")
)

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

func (a *dmlAdapter) walDataToQuery(d *wal.Data, generatedColumns []string) (*query, error) {
	switch d.Action {
	case "T":
		return a.buildTruncateQuery(d), nil
	case "D":
		return a.buildDeleteQuery(d)
	case "I":
		return a.buildInsertQuery(d, generatedColumns), nil
	case "U":
		return a.buildUpdateQuery(d, generatedColumns)
	default:
		return &query{}, nil
	}
}

func (a *dmlAdapter) buildTruncateQuery(d *wal.Data) *query {
	return &query{
		table:  d.Table,
		schema: d.Schema,
		sql:    fmt.Sprintf("TRUNCATE %s", quotedTableName(d.Schema, d.Table)),
	}
}

func (a *dmlAdapter) buildDeleteQuery(d *wal.Data) (*query, error) {
	whereQuery, whereValues, err := a.buildWhereQuery(d, 0)
	if err != nil {
		return nil, fmt.Errorf("building delete query: %w", err)
	}
	return &query{
		table:  d.Table,
		schema: d.Schema,
		sql:    fmt.Sprintf("DELETE FROM %s %s", quotedTableName(d.Schema, d.Table), whereQuery),
		args:   whereValues,
	}, nil
}

func (a *dmlAdapter) buildInsertQuery(d *wal.Data, generatedColumns []string) *query {
	names, values := a.filterRowColumns(d.Columns, generatedColumns)
	// if there are no columns after filtering generated ones, no query to run
	if len(names) == 0 {
		return &query{}
	}

	placeholders := make([]string, 0, len(d.Columns))
	for i := range names {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	return &query{
		table:       d.Table,
		schema:      d.Schema,
		columnNames: names,
		sql: fmt.Sprintf("INSERT INTO %s(%s) OVERRIDING SYSTEM VALUE VALUES(%s)%s",
			quotedTableName(d.Schema, d.Table), strings.Join(names, ", "),
			strings.Join(placeholders, ", "),
			a.buildOnConflictQuery(d, names)),
		args: values,
	}
}

func (a *dmlAdapter) buildUpdateQuery(d *wal.Data, generatedColumns []string) (*query, error) {
	rowColumns, rowValues := a.filterRowColumns(d.Columns, generatedColumns)
	// if there are no columns after filtering generated ones, no query to run
	if len(rowColumns) == 0 {
		return &query{}, nil
	}

	setQuery, setValues := a.buildSetQuery(d.Columns, rowColumns, rowValues)
	// if there are no columns after filtering generated ones, no query to run
	if setQuery == "" {
		return &query{}, nil
	}
	whereQuery, whereValues, err := a.buildWhereQuery(d, len(rowColumns))
	if err != nil {
		return nil, fmt.Errorf("building update query: %w", err)
	}

	return &query{
		table:  d.Table,
		schema: d.Schema,
		sql:    fmt.Sprintf("UPDATE %s %s %s", quotedTableName(d.Schema, d.Table), setQuery, whereQuery),
		args:   append(setValues, whereValues...),
	}, nil
}

func (a *dmlAdapter) buildWhereQuery(d *wal.Data, placeholderOffset int) (string, []any, error) {
	var cols []wal.Column
	switch {
	case len(d.Identity) > 0:
		// if we have the previous values (replica identity), add them to the where query
		cols = d.Identity
	case len(d.Metadata.InternalColIDs) > 0:
		// if we don't have previous values we have to rely on the primary keys
		primaryKeyCols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Columns)
		cols = primaryKeyCols
	default:
		// without a where clause in the query we'd be updating/deleting all table
		// rows, so we need to error to prevent that from happening
		return "", nil, errUnableToBuildQuery
	}

	whereQuery := "WHERE"
	whereValues := make([]any, 0, len(cols))
	for i, c := range cols {
		if i != 0 {
			whereQuery = fmt.Sprintf("%s AND", whereQuery)
		}
		whereQuery = fmt.Sprintf("%s %s = $%d", whereQuery, pglib.QuoteIdentifier(c.Name), i+placeholderOffset+1)
		whereValues = append(whereValues, c.Value)
	}
	return whereQuery, whereValues, nil
}

func (a *dmlAdapter) buildSetQuery(cols []wal.Column, rowColumns []string, rowValues []any) (string, []any) {
	setQuery := "SET"
	setValues := make([]any, 0, len(cols))
	for i, column := range rowColumns {
		if i != 0 {
			setQuery = fmt.Sprintf("%s,", setQuery)
		}
		setQuery = fmt.Sprintf("%s %s = $%d", setQuery, column, i+1)
		setValues = append(setValues, rowValues[i])
	}
	return setQuery, setValues
}

func (a *dmlAdapter) buildOnConflictQuery(d *wal.Data, filteredColumnNames []string) string {
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
		for _, col := range filteredColumnNames {
			cols = append(cols, fmt.Sprintf("%[1]s = EXCLUDED.%[1]s", col))
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
		colNames = append(colNames, pglib.QuoteIdentifier(col.Name))
	}
	return colNames
}

func (a *dmlAdapter) filterRowColumns(cols []wal.Column, generatedColumns []string) ([]string, []any) {
	generatedColumnMap := make(map[string]struct{}, len(generatedColumns))
	for _, col := range generatedColumns {
		generatedColumnMap[pglib.QuoteIdentifier(col)] = struct{}{}
	}
	// we need to make sure we only add the arguments for the
	// relevant column names (this removes any generated columns row values)
	rowValues := make([]any, 0, len(cols))
	rowColumns := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, found := generatedColumnMap[pglib.QuoteIdentifier(c.Name)]; found {
			continue
		}
		rowColumns = append(rowColumns, pglib.QuoteIdentifier(c.Name))
		rowValues = append(rowValues, c.Value)
	}
	return rowColumns, rowValues
}

func quotedTableName(schemaName, tableName string) string {
	return pglib.QuoteQualifiedIdentifier(schemaName, tableName)
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
