// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

func TestPostgresWriterConfigForProcessor_StrictMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		processorType processorType
		strictMode    bool
		want          bool
	}{
		{
			name:          "replication keeps default disabled",
			processorType: processorTypeReplication,
		},
		{
			name:          "replication preserves configured strict mode",
			processorType: processorTypeReplication,
			strictMode:    true,
			want:          true,
		},
		{
			name:          "snapshot enables strict mode by default",
			processorType: processorTypeSnapshot,
			want:          true,
		},
		{
			name:          "snapshot keeps strict mode enabled when configured",
			processorType: processorTypeSnapshot,
			strictMode:    true,
			want:          true,
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
