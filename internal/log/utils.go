// SPDX-License-Identifier: Apache-2.0

package log

import (
	"io"
	stdlog "log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Config struct {
	LogLevel string
}

// if we go over this limit the log will likely be truncated and it will not
// be very readable
const logMaxBytes = 10000

// init sets some zerolog global defaults we want to keep throughout the project.
func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "timestamp"
	zerolog.ErrorFieldName = "error.message"
	zerolog.ErrorStackFieldName = "error.stack"
	// remove v-level from zerologr wrapper.
	// The v-level is redundant with `level` emitted by zerolog.
	zerologr.VerbosityFieldName = ""

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return path.Base(file) + ":" + strconv.Itoa(line)
	}
}

func AddBytesToLog(log *zerolog.Event, key string, value []byte) *zerolog.Event {
	if len(value) > logMaxBytes {
		return log.Bytes(key, value[:logMaxBytes])
	}
	return log.Bytes(key, value)
}

// NewLogger creates a new logger writing to out.
// The logger will emit a timestamp, the caller's filename, and optionally
// emit the stacktrace for errors that carry a stack trace.
//
// The Debug and Trace level are samples.
// It allows up to 100 trace logs per minutes. Additional trace logs will be filtered out.
// Debug logs are sampled. Every 5th log will be filtered out once the limit of 1000 debug logs
// per minute is reached.
func NewLogger(config *Config) *zerolog.Logger {
	// ignore the error, it defaults to no level
	level, _ := zerolog.ParseLevel(config.LogLevel)
	out := zerolog.NewConsoleWriter(
		withTimeFormat(time.RFC3339Nano),
		withOut(os.Stderr),
	)

	logger := zerolog.New(out).
		Sample(zerolog.LevelSampler{
			TraceSampler: &zerolog.BurstSampler{
				Burst:  100,
				Period: 1 * time.Minute,
			},
			DebugSampler: &zerolog.BurstSampler{
				Burst:       1000,
				Period:      1 * time.Minute,
				NextSampler: &zerolog.BasicSampler{N: 5},
			},
		}).
		With().
		Timestamp().
		Caller().
		Stack().
		Logger().
		Level(level)

	return &logger
}

// SetGlobalLogger sets the log output in the stdlib log package and the
// zerolog global loggers.
func SetGlobalLogger(logger *zerolog.Logger) {
	// Rewire stdlib "log" global logger to our logger for dependencies
	// logging to `log.Default()...`
	stdlog.SetFlags(0)
	stdlog.SetOutput(logger)

	// Update zerolog global logger for packages/dependencies using this logger
	log.Logger = *logger

	// Set global logger in case context.Context is missing a contextual logger
	zerolog.DefaultContextLogger = logger
}

func withTimeFormat(format string) func(*zerolog.ConsoleWriter) {
	return func(w *zerolog.ConsoleWriter) {
		w.TimeFormat = format
	}
}

func withOut(out io.Writer) func(*zerolog.ConsoleWriter) {
	return func(w *zerolog.ConsoleWriter) {
		w.Out = out
	}
}
