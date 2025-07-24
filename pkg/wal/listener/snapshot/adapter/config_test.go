// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotConfig_schemaTableMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tables []string

		wantMap map[string][]string
	}{
		{
			name:   "ok",
			tables: []string{"a", "public.b", "test_schema.c"},
			wantMap: map[string][]string{
				"public":      {"a", "b"},
				"test_schema": {"c"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := SnapshotConfig{
				Tables: tc.tables,
			}
			got := schemaTableMap(config.Tables)
			require.Equal(t, tc.wantMap, got)
		})
	}
}
