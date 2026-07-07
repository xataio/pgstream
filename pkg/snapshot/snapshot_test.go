// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshot_GetTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot *Snapshot
		want     []string
	}{
		{
			name:     "nil snapshot",
			snapshot: nil,
			want:     nil,
		},
		{
			name:     "empty schema tables",
			snapshot: &Snapshot{SchemaTables: map[string][]string{}},
			want:     []string{},
		},
		{
			name: "single schema with tables",
			snapshot: &Snapshot{
				SchemaTables: map[string][]string{
					"public": {"users", "orders"},
				},
			},
			want: []string{"public.users", "public.orders"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ElementsMatch(t, tc.want, tc.snapshot.GetTables())
		})
	}
}
