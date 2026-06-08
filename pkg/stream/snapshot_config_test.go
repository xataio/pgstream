// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

func TestConfig_restoreConflictTargetsBeforeData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		onConflictAction string
		bulkIngest       bool
		noPostgres       bool

		want bool
	}{
		{
			name:             "update with batch writer restores constraints before data",
			onConflictAction: "update",
			want:             true,
		},
		{
			name:             "update with bulk ingest keeps default order",
			onConflictAction: "update",
			bulkIngest:       true,
		},
		{
			name:             "do nothing keeps default order",
			onConflictAction: "nothing",
		},
		{
			name: "default error behavior keeps default order",
		},
		{
			name:       "no postgres processor keeps default order",
			noPostgres: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := &Config{
				Processor: ProcessorConfig{},
			}
			if !tc.noPostgres {
				config.Processor.Postgres = &PostgresProcessorConfig{
					BatchWriter: pgwriter.Config{
						OnConflictAction:  tc.onConflictAction,
						BulkIngestEnabled: tc.bulkIngest,
					},
				}
			}

			require.Equal(t, tc.want, config.restoreConflictTargetsBeforeData())
		})
	}
}
