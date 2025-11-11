// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQuery_Size(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query *query

		wantSize int
	}{
		{
			name:  "nil query",
			query: nil,

			wantSize: 0,
		},
		{
			name: "empty query",
			query: &query{
				sql: "",
			},
			wantSize: 0,
		},
		{
			name: "simple query",
			query: &query{
				sql:         "SELECT * FROM users",
				columnNames: []string{"id", "name"},
			},
			wantSize: queryStructOverhead + len("SELECT * FROM users") + len("id") + len("name") + 2*stringOverhead,
		},
		{
			name: "query with parameters",
			query: &query{
				sql:  "SELECT * FROM users WHERE id = $1",
				args: []any{"string", []byte("bytes")},
			},
			wantSize: queryStructOverhead + len("SELECT * FROM users WHERE id = $1") + len("string") + len([]byte("bytes")) + 2*interfaceOverhead + sliceOverhead,
		},
		{
			name: "query with multiple parameters and column names",
			query: &query{
				sql:         "SELECT * FROM users WHERE id = $1",
				columnNames: []string{"id", "name"},
				args:        []any{123, time.Now()},
			},
			wantSize: queryStructOverhead + len("SELECT * FROM users WHERE id = $1") + len("id") + len("name") + 2*stringOverhead + 64 + 2*interfaceOverhead,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.query.Size()
			require.Equal(t, tc.wantSize, got)
		})
	}
}
