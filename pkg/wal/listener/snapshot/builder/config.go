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

// Option configures the snapshot generator built by NewSnapshotGenerator.
type Option func(*options)

type options struct {
	restoreConflictTargetsBeforeData bool
}

// WithRestoreConflictTargetsBeforeData makes the schema snapshot restore
// constraints/indexes usable as INSERT ... ON CONFLICT targets (primary keys,
// unique constraints and unique indexes) before the data snapshot runs.
func WithRestoreConflictTargetsBeforeData() Option {
	return func(o *options) {
		o.restoreConflictTargetsBeforeData = true
	}
}
