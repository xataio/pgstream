// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

func TestStore_DeleteSubscription(t *testing.T) {
	t.Parallel()

	testSubscription := &subscription.Subscription{
		URL:    "http://localhost:8080/webhook",
		Schema: "test_schema",
		Table:  "test_table",
	}
	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		execErr error

		wantErr error
	}{
		{
			name:    "ok",
			execErr: nil,
			wantErr: nil,
		},
		{
			name:    "error",
			execErr: errTest,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &Store{
				conn: &pgmocks.Querier{
					ExecFn: func(_ context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
						require.Equal(t, fmt.Sprintf(`DELETE FROM %s WHERE url=$1 AND schema_name=$2 AND table_name=$3;`, subscriptionsTable()), query)
						require.Equal(t, []any{testSubscription.URL, testSubscription.Schema, testSubscription.Table}, args)
						return pglib.CommandTag{}, tc.execErr
					},
				},
			}

			err := s.DeleteSubscription(context.Background(), testSubscription)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &Store{}
			query, params := s.buildGetQuery(tc.action, tc.schema, tc.table)
			require.Equal(t, tc.wantQuery, query)
			require.Equal(t, tc.wantParams, params)
		})
	}
}
