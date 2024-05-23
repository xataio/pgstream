// SPDX-License-Identifier: Apache-2.0

package log

import (
	"github.com/rs/zerolog"
)

// if we go over this limit the log will likely be truncated and it will not
// be very readable
const logMaxBytes = 10000

func AddBytesToLog(log *zerolog.Event, key string, value []byte) *zerolog.Event {
	if len(value) > logMaxBytes {
		return log.Bytes(key, value[:logMaxBytes])
	}
	return log.Bytes(key, value)
}
