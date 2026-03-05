// SPDX-License-Identifier: Apache-2.0

package builder

import (
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
)

type SnapshotListenerConfig struct {
	Data                    *pgsnapshotgenerator.Config
	Adapter                 adapter.SnapshotConfig
	Recorder                *SnapshotRecorderConfig
	Schema                  *SchemaSnapshotConfig
	DisableProgressTracking bool
}

type SchemaSnapshotConfig struct {
	DumpRestore *pgdumprestore.Config
}

type SnapshotRecorderConfig struct {
	RepeatableSnapshots bool
	SnapshotWorkers     uint
	SnapshotStoreURL    string
}
