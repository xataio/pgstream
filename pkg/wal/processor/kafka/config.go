// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	Kafka kafka.ConnConfig
	Batch batch.Config
}
