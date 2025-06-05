// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sync"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
)

type pgColumnObserver struct {
	pgConn                     pglib.Querier
	generatedTableColumns      map[string][]string
	generatedTableColumnsMutex *sync.RWMutex
}

func newPGColumnObserver(ctx context.Context, pgURL string) (*pgColumnObserver, error) {
	pgConn, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &pgColumnObserver{
		pgConn:                     pgConn,
		generatedTableColumns:      map[string][]string{},
		generatedTableColumnsMutex: &sync.RWMutex{},
	}, nil
}

func (o *pgColumnObserver) getGeneratedColumnNames(ctx context.Context, schema, table string) ([]string, error) {
	key := pglib.QuoteQualifiedIdentifier(schema, table)
	o.generatedTableColumnsMutex.RLock()
	columns, found := o.generatedTableColumns[key]
	o.generatedTableColumnsMutex.RUnlock()
	if found {
		return columns, nil
	}

	//  if not found in the map, retrieve them from postgres
	colNames, err := o.queryGeneratedColumnNames(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	o.generatedTableColumnsMutex.Lock()
	o.generatedTableColumns[key] = colNames
	o.generatedTableColumnsMutex.Unlock()
	return colNames, nil
}

func (o *pgColumnObserver) updateGeneratedColumnNames(logEntry *schemalog.LogEntry) {
	for _, table := range logEntry.Schema.Tables {
		key := pglib.QuoteQualifiedIdentifier(logEntry.SchemaName, table.Name)
		generatedColumns := make([]string, 0, len(table.Columns))
		for _, c := range table.Columns {
			if c.Generated {
				generatedColumns = append(generatedColumns, c.Name)
			}
		}

		o.generatedTableColumnsMutex.Lock()
		o.generatedTableColumns[key] = generatedColumns
		o.generatedTableColumnsMutex.Unlock()
	}
}

const generatedTableColumnsQuery = `SELECT attname FROM pg_attribute
		WHERE attnum > 0
		AND attrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2)
		AND attgenerated != ''`

func (o *pgColumnObserver) queryGeneratedColumnNames(ctx context.Context, schemaName, tableName string) ([]string, error) {
	columnNames := []string{}
	// filter out generated columns (excluding identities) since they will
	// be generated automatically, and they can't be overwriten.
	rows, err := o.pgConn.Query(ctx, generatedTableColumnsQuery, tableName, schemaName)
	if err != nil {
		return nil, fmt.Errorf("getting table generated column names for table %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scanning table generated column name: %w", err)
		}
		columnNames = append(columnNames, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columnNames, nil
}

func (o *pgColumnObserver) close() error {
	return o.pgConn.Close(context.Background())
}
