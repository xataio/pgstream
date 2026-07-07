// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

func TestPostgresWriterConfigForProcessor_StrictMode(t *testing.T) {
	t.Parallel()

	boolPtr := func(b bool) *bool { return &b }

	tests := []struct {
		name          string
		processorType processorType
		strictMode    *bool
		want          *bool
	}{
		{
			name:          "replication keeps default disabled",
			processorType: processorTypeReplication,
			want:          nil,
		},
		{
			name:          "replication preserves configured strict mode",
			processorType: processorTypeReplication,
			strictMode:    boolPtr(true),
			want:          boolPtr(true),
		},
		{
			name:          "snapshot enables strict mode by default",
			processorType: processorTypeSnapshot,
			want:          boolPtr(true),
		},
		{
			name:          "snapshot keeps strict mode enabled when configured",
			processorType: processorTypeSnapshot,
			strictMode:    boolPtr(true),
			want:          boolPtr(true),
		},
		{
			name:          "snapshot honors explicitly disabled strict mode",
			processorType: processorTypeSnapshot,
			strictMode:    boolPtr(false),
			want:          boolPtr(false),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := postgresWriterConfigForProcessor(pgwriter.Config{
				StrictMode: tc.strictMode,
			}, tc.processorType)
			require.Equal(t, tc.want, got.StrictMode)
		})
	}
}
