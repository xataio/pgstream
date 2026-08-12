// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObserverConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		maxConnections int32
		expected       int32
	}{
		{
			name:           "default writer pool is capped",
			maxConnections: 50,
			expected:       maxObserverConnections,
		},
		{
			name:           "large writer pool is capped",
			maxConnections: 500,
			expected:       maxObserverConnections,
		},
		{
			name:           "small writer pool bounds the observer",
			maxConnections: 4,
			expected:       4,
		},
		{
			name:           "exactly at the cap",
			maxConnections: maxObserverConnections,
			expected:       maxObserverConnections,
		},
		{
			name:           "single connection writer pool",
			maxConnections: 1,
			expected:       1,
		},
		{
			// pgxpool rejects pools smaller than one
			name:           "never returns less than one",
			maxConnections: 0,
			expected:       1,
		},
		{
			name:           "negative writer pool still floors at one",
			maxConnections: -7,
			expected:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, observerConnections(tt.maxConnections))
		})
	}
}
