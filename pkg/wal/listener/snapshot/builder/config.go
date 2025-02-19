// SPDX-License-Identifier: Apache-2.0

package builder

import (
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
)

type SnapshotListenerConfig struct {
	Generator        pgsnapshotgenerator.Config
	Adapter          adapter.SnapshotConfig
	SnapshotStoreURL string
	Schema           SchemaSnapshotConfig
}

type SchemaSnapshotConfig struct {
	SchemaLogStore *schemalogpg.Config
	DumpRestore    *pgdumprestore.Config
}
