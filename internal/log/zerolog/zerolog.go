// SPDX-License-Identifier: Apache-2.0

package zerolog

import (
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path"
	"strconv"
	"time"

	loglib "github.com/xataio/pgstream/pkg/log"
	zerologlib "github.com/xataio/pgstream/pkg/log/zerolog"

	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	formatConsole = "console"
	formatJSON    = "json"
)

type Config struct {
	// LogLevel selects the minimum log level emitted. Supported values are
	// trace, debug, info, warn, error, fatal, panic, and disabled. An empty
	// string disables level filtering (every event is written).
	LogLevel string
	// LogFormat selects the output encoding. Supported values are "console"
	// (human-readable) and "json" (structured, one JSON object per line).
	// An empty string defaults to "console". Unknown values also fall back
	// to "console"; CLI input is validated up front by Config.Validate.
	LogFormat string
	// NoColor disables ANSI colors in the "console" format. Ignored when
	// LogFormat is "json".
	NoColor bool
}

// ErrNoColorUnderJSONFormat is returned by Validate when NoColor is enabled
// while LogFormat is "json"; the option only applies to the console writer.
var ErrNoColorUnderJSONFormat = fmt.Errorf("no_color is only valid when log format is 'console'")

// Validate returns an error if the Config contains an unsupported value.
// Empty fields are treated as defaults and accepted.
func (c *Config) Validate() error {
	switch c.LogFormat {
	case "", formatConsole, formatJSON:
	default:
		return fmt.Errorf("invalid log format %q: must be one of console, json", c.LogFormat)
	}
	if c.NoColor && c.LogFormat == formatJSON {
		return ErrNoColorUnderJSONFormat
	}
	return nil
}

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

func NewStdLogger(l *zerolog.Logger) loglib.Logger {
	return zerologlib.NewLogger(l)
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

	var out io.Writer
	if config.LogFormat == formatJSON {
		out = os.Stderr
	} else {
		out = zerolog.NewConsoleWriter(
			withTimeFormat(time.RFC3339Nano),
			withOut(os.Stderr),
			withNoColor(config.NoColor),
		)
	}

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

func withNoColor(noColor bool) func(*zerolog.ConsoleWriter) {
	return func(w *zerolog.ConsoleWriter) {
		w.NoColor = noColor
	}
}
