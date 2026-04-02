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
// Composite PK: DELETE FROM t WHERE (a,b) IN (($1,$2),($3,$4),...) split at maxParamsPerQuery
// NULL identity values are handled as individual queries.
func (a *dmlAdapter) buildBulkDeleteQuery(events []*wal.Data) ([]*query, error) {
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
		q, err := a.buildBulkDeleteSinglePK(normalEvents, firstCols[0], tableName)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	} else {
		// composite PK: use IN tuples, split at maxParamsPerQuery
		qs, err := a.buildBulkDeleteCompositePK(normalEvents, numPKCols, tableName)
		if err != nil {
			return nil, err
		}
		queries = append(queries, qs...)
	}

	return queries, nil
}

func (a *dmlAdapter) buildBulkDeleteSinglePK(events []*wal.Data, refCol wal.Column, tableName string) (*query, error) {
	values := make([]any, 0, len(events))
	for _, e := range events {
		cols, err := a.getIdentityColumns(e)
		if err != nil {
			return nil, err
		}
		values = append(values, serializeJSONBValue(cols[0].Type, cols[0].Value))
	}

	colName := pglib.QuoteIdentifier(refCol.Name)
	arrayType := pgArrayType(refCol.Type)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1::%s)", tableName, colName, arrayType)

	return &query{
		schema: events[0].Schema,
		table:  events[0].Table,
		sql:    sql,
		args:   []any{values},
	}, nil
}

func (a *dmlAdapter) buildBulkDeleteCompositePK(events []*wal.Data, numPKCols int, tableName string) ([]*query, error) {
	var queries []*query

	// split events into chunks to stay under maxParamsPerQuery
	eventsPerChunk := max(maxParamsPerQuery/numPKCols, 1)

	for start := 0; start < len(events); start += eventsPerChunk {
		end := min(start+eventsPerChunk, len(events))
		chunk := events[start:end]

		// get column names from first event in chunk
		firstCols, err := a.getIdentityColumns(chunk[0])
		if err != nil {
			return nil, err
		}

		colNames := make([]string, numPKCols)
		for i, c := range firstCols {
			colNames[i] = pglib.QuoteIdentifier(c.Name)
		}

		args := make([]any, 0, len(chunk)*numPKCols)
		tuples := make([]string, 0, len(chunk))
		paramIdx := 0

		for _, e := range chunk {
			cols, err := a.getIdentityColumns(e)
			if err != nil {
				return nil, err
			}

			placeholders := make([]string, numPKCols)
			for i, c := range cols {
				paramIdx++
				placeholders[i] = fmt.Sprintf("$%d", paramIdx)
				args = append(args, serializeJSONBValue(c.Type, c.Value))
			}
			tuples = append(tuples, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))
		}

		sql := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
			tableName,
			strings.Join(colNames, ","),
			strings.Join(tuples, ","))

		queries = append(queries, &query{
			schema: chunk[0].Schema,
			table:  chunk[0].Table,
			sql:    sql,
			args:   args,
		})
	}

	return queries, nil
}

// buildBulkInsertQueries coalesces multiple INSERT events for the same table
// into multi-row INSERT statements, split at maxParamsPerQuery.
// It also emits a single setval per sequence using the max value across all events.
func (a *dmlAdapter) buildBulkInsertQueries(events []*wal.Data, si schemaInfo) []*query {
	if len(events) == 0 {
		return nil
	}

	// determine column names from first event
	names, _ := a.filterRowColumns(events[0].Columns, si)
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
						colValueFloat, ok := col.Value.(float64)
						if !ok {
							a.logger.Warn(nil, "unexpected value type for sequence column, expected integer", loglib.Fields{
								"column_name": col.Name, "column_type": col.Type, "column_value": col.Value,
							})
							continue
						}
						val := int64(colValueFloat)
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
			schema:      events[0].Schema,
			table:       events[0].Table,
			columnNames: names,
			sql:         sql,
			args:        args,
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
