// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
)

func makeLogger(start func() *zerolog.Event) kafka.LoggerFunc {
	return func(msg string, args ...interface{}) {
		start().Msgf(msg, args...)
	}
}
