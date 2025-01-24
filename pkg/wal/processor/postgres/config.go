// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	URL         string
	BatchConfig batch.Config
}
