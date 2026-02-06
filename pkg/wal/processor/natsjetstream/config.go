// SPDX-License-Identifier: Apache-2.0

package natsjetstream

import (
	natslib "github.com/xataio/pgstream/pkg/nats"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	NATS  natslib.ConnConfig
	Batch batch.Config
}
