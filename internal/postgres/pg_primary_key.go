// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
)

// PrimaryKeyColumnsQuery returns the primary-key columns of a table, ordered by
// their position in the primary-key definition.
const PrimaryKeyColumnsQuery = `
	SELECT a.attname
	FROM pg_index i
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE i.indrelid = to_regclass(quote_ident($1) || '.' || quote_ident($2))
	  AND i.indisprimary
	ORDER BY array_position(i.indkey::smallint[], a.attnum)`

func GetPrimaryKeyColumns(ctx context.Context, conn Querier, schema, table string) ([]string, error) {
	rows, err := conn.Query(ctx, PrimaryKeyColumnsQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("querying primary key columns for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	columns := []string{}
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scanning primary key column name: %w", err)
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating primary key columns for %s.%s: %w", schema, table, err)
	}

	return columns, nil
}
