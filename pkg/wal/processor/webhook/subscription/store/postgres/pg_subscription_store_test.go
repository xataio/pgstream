// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStore_buildGetQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		schema string
		table  string

		wantQuery  string
		wantParams []any
	}{
		{
			name:       "no filters",
			wantQuery:  fmt.Sprintf(`SELECT url, schema_name, table_name, event_types FROM %s LIMIT 1000`, subscriptionsTable()),
			wantParams: nil,
		},
		{
			name:       "with action filter",
			action:     "I",
			wantQuery:  fmt.Sprintf(`SELECT url, schema_name, table_name, event_types FROM %s WHERE ($1=ANY(event_types) OR event_types IS NULL) LIMIT 1000`, subscriptionsTable()),
			wantParams: []any{"I"},
		},
		{
			name:       "with schema filter",
			schema:     "test_schema",
			wantQuery:  fmt.Sprintf(`SELECT url, schema_name, table_name, event_types FROM %s WHERE (schema_name=$1 OR schema_name='') LIMIT 1000`, subscriptionsTable()),
			wantParams: []any{"test_schema"},
		},
		{
			name:       "with table filter",
			table:      "test_table",
			wantQuery:  fmt.Sprintf(`SELECT url, schema_name, table_name, event_types FROM %s WHERE (table_name=$1 OR table_name='') LIMIT 1000`, subscriptionsTable()),
			wantParams: []any{"test_table"},
		},
		{
			name:   "with all filters",
			action: "I",
			schema: "test_schema",
			table:  "test_table",
			wantQuery: fmt.Sprintf(`SELECT url, schema_name, table_name, event_types FROM %s `, subscriptionsTable()) +
				"WHERE (schema_name=$1 OR schema_name='') " +
				"AND (table_name=$2 OR table_name='') " +
				"AND ($3=ANY(event_types) OR event_types IS NULL) LIMIT 1000",
			wantParams: []any{"test_schema", "test_table", "I"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &Store{}
			query, params := s.buildGetQuery(tc.action, tc.schema, tc.table)
			require.Equal(t, tc.wantQuery, query)
			require.Equal(t, tc.wantParams, params)
		})
	}
}
