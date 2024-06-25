// SPDX-License-Identifier: Apache-2.0

package zerolog

import (
	"time"

	"github.com/rs/zerolog"

	loglib "github.com/xataio/pgstream/pkg/log"
)

type Logger struct {
	zerologger *zerolog.Logger
	fields     loglib.Fields
}

// if we go over this limit the log will likely be truncated and it will not
// be very readable
const logMaxBytes = 10000

func NewLogger(zl *zerolog.Logger) *Logger {
	return &Logger{
		zerologger: zl,
	}
}

func (l *Logger) Trace(msg string, fields ...loglib.Fields) {
	withFields(l.zerologger.Trace(), append(fields, l.fields)...).Msg(msg)
}

func (l *Logger) Debug(msg string, fields ...loglib.Fields) {
	withFields(l.zerologger.Debug(), append(fields, l.fields)...).Msg(msg)
}

func (l *Logger) Info(msg string, fields ...loglib.Fields) {
	withFields(l.zerologger.Info(), append(fields, l.fields)...).Msg(msg)
}

func (l *Logger) Warn(err error, msg string, fields ...loglib.Fields) {
	withFields(l.zerologger.Warn().Err(err), append(fields, l.fields)...).Msg(msg)
}

func (l *Logger) Error(err error, msg string, fields ...loglib.Fields) {
	withFields(l.zerologger.Error().Err(err), append(fields, l.fields)...).Msg(msg)
}

func (l *Logger) Panic(msg string, fields ...loglib.Fields) {
	withFields(l.zerologger.Panic(), append(fields, l.fields)...).Msg(msg)
}

func (l *Logger) WithFields(fields loglib.Fields) loglib.Logger {
	return &Logger{
		zerologger: l.zerologger,
		fields:     loglib.MergeFields(l.fields, fields),
	}
}

func withFields(event *zerolog.Event, fieldMaps ...loglib.Fields) *zerolog.Event {
	if len(fieldMaps) == 0 {
		return event
	}
	for _, m := range fieldMaps {
		for key, value := range m {
			switch v := value.(type) {
			case string:
				event = event.Str(key, v)
			case int:
				event = event.Int(key, v)
			case int32:
				event = event.Int32(key, v)
			case int64:
				event = event.Int64(key, v)
			case []byte:
				event = addBytesToLog(event, key, v)
			case time.Time:
				event = event.Time(key, v)
			case time.Duration:
				event = event.Dur(key, v)
			case []string:
				event = event.Strs(key, v)
			default:
				event = event.Any(key, v)
			}
		}
	}
	return event
}

func addBytesToLog(log *zerolog.Event, key string, value []byte) *zerolog.Event {
	if len(value) > logMaxBytes {
		return log.Bytes(key, value[:logMaxBytes])
	}
	return log.Bytes(key, value)
}
