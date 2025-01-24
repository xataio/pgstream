// SPDX-License-Identifier: Apache-2.0

package search

import (
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type IndexerConfig struct {
	Batch batch.Config
}
