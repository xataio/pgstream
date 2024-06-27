// SPDX-License-Identifier: Apache-2.0

package subscription

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubscription_IsFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		subscription *Subscription
		name         string
		action       string
		schema       string
		table        string

		wantMatch bool
	}{
		{
			name:         "wildcards for all fields, subscription matched",
			subscription: newTestSubscription("url-1", "test_schema", "test_table", []string{"I"}),
			action:       "",
			schema:       "",
			table:        "",
			wantMatch:    true,
		},
		{
			name:         "all fields provided, subscription matched",
			subscription: newTestSubscription("url-1", "test_schema", "test_table", []string{"I"}),
			action:       "I",
			schema:       "test_schema",
			table:        "test_table",
			wantMatch:    true,
		},
		{
			name:         "wildcard subscription, subscription matched",
			subscription: newTestSubscription("url-1", "", "", []string{}),
			action:       "I",
			schema:       "test_schema",
			table:        "test_table",
			wantMatch:    true,
		},
		{
			name:         "filter by action, subscription not matched",
			subscription: newTestSubscription("url-1", "test_schema", "test_table", []string{"I"}),
			action:       "D",
			wantMatch:    false,
		},
		{
			name:         "filter by schema, subscription not matched",
			subscription: newTestSubscription("url-1", "test_schema", "test_table", []string{"I"}),
			schema:       "another_schema",
			wantMatch:    false,
		},
		{
			name:         "filter by table, subscription not matched",
			subscription: newTestSubscription("url-1", "test_schema", "test_table", []string{"I"}),
			table:        "another_table",
			wantMatch:    false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			match := tc.subscription.IsFor(tc.action, tc.schema, tc.table)
			require.Equal(t, tc.wantMatch, match)
		})
	}
}

func newTestSubscription(url, schema, table string, eventTypes []string) *Subscription {
	return &Subscription{
		URL:        url,
		Schema:     schema,
		Table:      table,
		EventTypes: eventTypes,
	}
}
