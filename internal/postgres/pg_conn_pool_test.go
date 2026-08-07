// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConnPool_maxConnections(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		opts     []PoolOption
		expected int32
	}{
		{
			name:     "default",
			url:      "postgresql://user:password@localhost:5432/database",
			expected: MaxConns,
		},
		{
			name:     "connection URL",
			url:      "postgresql://user:password@localhost:5432/database?pool_max_conns=12",
			expected: 12,
		},
		{
			name:     "pool option overrides connection URL",
			url:      "postgresql://user:password@localhost:5432/database?pool_max_conns=12",
			opts:     []PoolOption{WithMaxConnections(24)},
			expected: 24,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewConnPool(t.Context(), tt.url, tt.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pool.Close(t.Context())) })

			require.Equal(t, tt.expected, pool.Config().MaxConns)
		})
	}
}
