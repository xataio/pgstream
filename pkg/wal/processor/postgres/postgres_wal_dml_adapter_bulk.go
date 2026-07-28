// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

const maxParamsPerQuery = 60000

// pgArrayType maps a PostgreSQL scalar type to its corresponding array cast
// type for use in ANY($1::type[]) expressions.
func pgArrayType(colType string) string {
	switch colType {
	case "integer", "int4":
		return "int4[]"
	case "bigint", "int8":
		return "int8[]"
	case "smallint", "int2":
		return "int2[]"
	case "text":
		return "text[]"
	case "uuid":
		return "uuid[]"
	case "character varying", "varchar":
		return "text[]"
	default:
		return colType + "[]"
	}
}

// getIdentityColumns returns the columns used for identifying rows (for
// WHERE clauses). It prefers Identity (replica identity) over InternalColIDs.
func (a *dmlAdapter) getIdentityColumns(d *wal.Data) ([]wal.Column, error) {
	switch {
	case len(d.Identity) > 0:
		return d.Identity, nil
	case len(d.Metadata.InternalColIDs) > 0:
		cols := a.extractPrimaryKeyColumns(d.Metadata.InternalColIDs, d.Columns)
		if len(cols) == 0 {
			return nil, errUnableToBuildQuery
		}
		return cols, nil
	default:
		return nil, errUnableToBuildQuery
	}
}

// buildBulkDeleteQuery coalesces multiple DELETE events for the same table
// into as few queries as possible.
//
// Single PK: DELETE FROM t WHERE col = ANY($1::type[])
// Composite PK: DELETE FROM t WHERE (a,b) IN (SELECT * FROM unnest($1::a[], $2::b[]))
// NULL identity values are handled as individual queries.
//
// Identity columns whose type is a user-defined enum are bound as text[] and
// cast back on the target instead, see bindAsText.
func (a *dmlAdapter) buildBulkDeleteQuery(events []*wal.Data, si schemaInfo) ([]*query, error) {
	if len(events) == 0 {
		return nil, nil
	}

	// determine identity columns from the first event
	firstCols, err := a.getIdentityColumns(events[0])
	if err != nil {
		return nil, fmt.Errorf("building bulk delete query: %w", err)
	}

	tableName := quotedTableName(events[0].Schema, events[0].Table)
	numPKCols := len(firstCols)

	// separate events with NULL identity values — they need individual queries
	var normalEvents []*wal.Data
	var nullEvents []*wal.Data

	for _, e := range events {
		cols, err := a.getIdentityColumns(e)
		if err != nil {
			return nil, fmt.Errorf("building bulk delete query: %w", err)
		}
		hasNull := false
		for _, c := range cols {
			if c.Value == nil {
				hasNull = true
				break
			}
		}
		if hasNull {
			nullEvents = append(nullEvents, e)
		} else {
			normalEvents = append(normalEvents, e)
		}
	}

	queries := make([]*query, 0, len(nullEvents)+1)

	// handle NULL identity events individually
	for _, e := range nullEvents {
		q, err := a.buildDeleteQuery(e)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	}

	if len(normalEvents) == 0 {
		return queries, nil
	}

	if numPKCols == 1 {
		// single PK: use ANY($1::type[])
		q, err := a.buildBulkDeleteSinglePK(normalEvents, firstCols[0], tableName, si)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	} else {
		// composite PK: use unnest of one array per PK column
		q, err := a.buildBulkDeleteCompositePK(normalEvents, numPKCols, tableName, si)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	}

	return queries, nil
}

// bindAsText reports whether the identity values for the given column must be
// bound as a text[] parameter and cast back on the target, rather than bound
// directly as an array of the column's own type. The returned enumColumn
// carries the catalog-resolved cast information and is only meaningful when the
// second return value is true.
//
// columnName must be quoted to match the enumColumns set.
func bindAsText(columnName string, si schemaInfo) (enumColumn, bool) {
	col, isEnum := si.enumColumns[columnName]
	return col, isEnum
}

// enumComparison renders the comparison between an enum-resolving identity
// column and a text[] parameter, casting whichever sides the column's shape
// requires. Cast targets come from the target catalog (see enumColumn), never
// from the replication stream, so they are safe to interpolate.
func enumComparison(colName string, col enumColumn, param string) string {
	switch {
	case col.isArray:
		// ANY unwraps one array level, which would compare the element type
		// against an array-typed column ("operator does not exist:
		// my_enum[] = my_enum"), so compare whole arrays with IN (SELECT ...).
		return fmt.Sprintf("%s IN (SELECT unnest(%s::text[])::%s[])", colName, param, col.enumType)
	case col.isDomain:
		// the enum comparison operators are polymorphic over anyenum, which
		// does not accept a domain, so the column side must be cast too. That
		// forfeits a plain index on the column, but no index-friendly form
		// exists: uncast, postgres reports "operator does not exist:
		// my_domain = my_enum".
		return fmt.Sprintf("%s::%s = ANY(%s::text[]::%s[])", colName, col.enumType, param, col.enumType)
	default:
		// the cast stays entirely on the parameter side, leaving the column
		// side untouched so an index on it remains usable (verified: Index
		// Cond: (col = ANY (('{...}'::text[])::my_enum[]))).
		return fmt.Sprintf("%s = ANY(%s::text[]::%s[])", colName, param, col.enumType)
	}
}

func (a *dmlAdapter) buildBulkDeleteSinglePK(events []*wal.Data, refCol wal.Column, tableName string, si schemaInfo) (*query, error) {
	values := make([]any, 0, len(events))
	for _, e := range events {
		cols, err := a.getIdentityColumns(e)
		if err != nil {
			return nil, err
		}
		values = append(values, serializeJSONBValue(cols[0].Type, cols[0].Value))
	}

	colName := pglib.QuoteIdentifier(refCol.Name)
	var sql string
	if enumCol, isEnum := bindAsText(colName, si); isEnum {
		sql = fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, enumComparison(colName, enumCol, "$1"))
	} else {
		sql = fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1::%s)", tableName, colName, pgArrayType(refCol.Type))
	}

	return &query{
		schema: events[0].Schema,
		table:  events[0].Table,
		sql:    sql,
		args:   []any{values},
	}, nil
}

// buildBulkDeleteCompositePK emits a single stack-safe DELETE for composite-PK
// tables:
//
//	DELETE FROM t WHERE (a,b) IN (SELECT * FROM unnest($1::a[], $2::b[]))
//
// One array parameter is bound per PK column, so the parameter count is
// constant (numPKCols) regardless of how many rows are deleted.
//
// Columns that must be bound as text[] (see bindAsText) are cast back inside
// the unnest projection, which requires naming the unnest output columns:
//
//	DELETE FROM t WHERE (a,b) IN (SELECT c1,c2::my_enum FROM unnest($1::a[],$2::text[]) AS u(c1,c2))
//
// A domain over an enum additionally needs the column side cast, for the
// anyenum reason described on enumComparison.
func (a *dmlAdapter) buildBulkDeleteCompositePK(events []*wal.Data, numPKCols int, tableName string, si schemaInfo) (*query, error) {
	// determine column names + types from the first event
	firstCols, err := a.getIdentityColumns(events[0])
	if err != nil {
		return nil, err
	}

	colNames := make([]string, numPKCols)
	unnestArgs := make([]string, numPKCols)
	unnestAliases := make([]string, numPKCols)
	selectItems := make([]string, numPKCols)
	needsCast := false
	// one array per PK column, gathered across all events
	colValues := make([][]any, numPKCols)
	for i, c := range firstCols {
		colNames[i] = pglib.QuoteIdentifier(c.Name)
		unnestAliases[i] = fmt.Sprintf("c%d", i+1)
		enumCol, isEnum := bindAsText(colNames[i], si)
		switch {
		case !isEnum:
			unnestArgs[i] = fmt.Sprintf("$%d::%s", i+1, pgArrayType(c.Type))
			selectItems[i] = unnestAliases[i]
		default:
			needsCast = true
			unnestArgs[i] = fmt.Sprintf("$%d::text[]", i+1)
			castType := enumCol.enumType
			if enumCol.isArray {
				castType += "[]"
			}
			selectItems[i] = fmt.Sprintf("%s::%s", unnestAliases[i], castType)
			if enumCol.isDomain && !enumCol.isArray {
				colNames[i] = fmt.Sprintf("%s::%s", colNames[i], enumCol.enumType)
			}
		}
		colValues[i] = make([]any, 0, len(events))
	}

	for _, e := range events {
		cols, err := a.getIdentityColumns(e)
		if err != nil {
			return nil, err
		}
		for i, c := range cols {
			colValues[i] = append(colValues[i], serializeJSONBValue(c.Type, c.Value))
		}
	}

	args := make([]any, numPKCols)
	for i := range colValues {
		args[i] = colValues[i]
	}

	// only name the unnest output columns when a cast needs to reference them,
	// so the emitted SQL is unchanged for tables without such columns.
	unnestSelect := fmt.Sprintf("SELECT * FROM unnest(%s)", strings.Join(unnestArgs, ","))
	if needsCast {
		unnestSelect = fmt.Sprintf("SELECT %s FROM unnest(%s) AS u(%s)",
			strings.Join(selectItems, ","),
			strings.Join(unnestArgs, ","),
			strings.Join(unnestAliases, ","))
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
		tableName,
		strings.Join(colNames, ","),
		unnestSelect)

	return &query{
		schema: events[0].Schema,
		table:  events[0].Table,
		sql:    sql,
		args:   args,
	}, nil
}

// buildBulkInsertQueries coalesces multiple INSERT events for the same table
// into multi-row INSERT statements, split at maxParamsPerQuery.
// It also emits a single setval per sequence using the max value across all events.
func (a *dmlAdapter) buildBulkInsertQueries(events []*wal.Data, si schemaInfo) []*query {
	if len(events) == 0 {
		return nil
	}

	// determine column names + types from first event so the writer can
	// decide between binary and text COPY downstream.
	names, types, _ := a.filterRowColumnsWithTypes(events[0].Columns, si)
	if len(names) == 0 {
		return []*query{}
	}

	numCols := len(names)
	rowsPerChunk := max(maxParamsPerQuery/numCols, 1)

	numChunks := (len(events) + rowsPerChunk - 1) / rowsPerChunk
	queries := make([]*query, 0, numChunks+len(events))

	// track max values for sequence columns across all events
	seqMaxValues := make(map[string]int64) // seqName -> maxValue

	for start := 0; start < len(events); start += rowsPerChunk {
		end := min(start+rowsPerChunk, len(events))
		chunk := events[start:end]

		args := make([]any, 0, len(chunk)*numCols)
		valueTuples := make([]string, 0, len(chunk))
		paramIdx := 0

		for _, e := range chunk {
			_, values := a.filterRowColumns(e.Columns, si)
			placeholders := make([]string, numCols)
			for i, v := range values {
				paramIdx++
				placeholders[i] = fmt.Sprintf("$%d", paramIdx)
				args = append(args, v)
			}
			valueTuples = append(valueTuples, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))

			// track sequence max values
			if !a.forCopy {
				for _, col := range e.Columns {
					if seqName, ok := si.sequenceColumns[pglib.QuoteIdentifier(col.Name)]; ok {
						val, ok := toInt64(col.Value)
						if !ok {
							a.logger.Warn(nil, "unexpected value type for sequence column, expected integer", loglib.Fields{
								"column_name": col.Name, "column_type": col.Type, "column_value": col.Value,
							})
							continue
						}
						if current, exists := seqMaxValues[seqName]; !exists || val > current {
							seqMaxValues[seqName] = val
						}
					}
				}
			}
		}

		sql := fmt.Sprintf("INSERT INTO %s(%s) OVERRIDING SYSTEM VALUE VALUES%s%s",
			quotedTableName(events[0].Schema, events[0].Table),
			strings.Join(names, ", "),
			strings.Join(valueTuples, ","),
			a.buildOnConflictQuery(events[0], names))

		queries = append(queries, &query{
			schema:        events[0].Schema,
			table:         events[0].Table,
			columnNames:   names,
			needsTextCopy: needsTextCopyForColumns(names, types, si.enumColumns),
			sql:           sql,
			args:          args,
		})
	}

	// emit a single setval per sequence using the max value
	for seqName, maxVal := range seqMaxValues {
		queries = append(queries, &query{
			table:  events[0].Table,
			schema: events[0].Schema,
			sql:    "SELECT setval($1::regclass, $2::bigint, true)",
			args:   []any{seqName, maxVal},
		})
	}

	return queries
}
