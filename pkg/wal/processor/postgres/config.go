// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	URL              string
	BatchConfig      batch.Config
	SchemaLogStore   schemalogpg.Config
	DisableTriggers  bool
	OnConflictAction string
}
