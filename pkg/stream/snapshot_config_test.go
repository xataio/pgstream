// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

func TestPrepareSnapshotSchemaRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		onConflictAction string
		bulkIngest       bool

		wantRestoreConstraintsBeforeData bool
	}{
		{
			name:                             "update with batch writer restores constraints before data",
			onConflictAction:                 "update",
			wantRestoreConstraintsBeforeData: true,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := &Config{
				Listener: ListenerConfig{
					Postgres: &PostgresListenerConfig{
						Snapshot: &snapshotbuilder.SnapshotListenerConfig{
							Data: &pgsnapshotgenerator.Config{},
							Schema: &snapshotbuilder.SchemaSnapshotConfig{
								DumpRestore: &pgdumprestore.Config{},
							},
						},
					},
				},
				Processor: ProcessorConfig{
					Postgres: &PostgresProcessorConfig{
						BatchWriter: pgwriter.Config{
							OnConflictAction:  tc.onConflictAction,
							BulkIngestEnabled: tc.bulkIngest,
						},
					},
				},
			}

			prepareSnapshotSchemaRestore(config)

			require.Equal(t,
				tc.wantRestoreConstraintsBeforeData,
				config.Listener.Postgres.Snapshot.Schema.DumpRestore.RestoreIndicesAndConstraintsBeforeData)
		})
	}
}
