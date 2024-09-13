// SPDX-License-Identifier: Apache-2.0

package log

type Logger interface {
	Trace(msg string, fields ...Fields)
	Debug(msg string, fields ...Fields)
	Info(msg string, fields ...Fields)
	Warn(err error, msg string, fields ...Fields)
	Error(err error, msg string, fields ...Fields)
	Panic(msg string, fields ...Fields)
	WithFields(fields Fields) Logger
}

type Fields map[string]any

type NoopLogger struct{}

func (l *NoopLogger) Trace(msg string, fields ...Fields)            {}
func (l *NoopLogger) Debug(msg string, fields ...Fields)            {}
func (l *NoopLogger) Info(msg string, fields ...Fields)             {}
func (l *NoopLogger) Warn(err error, msg string, fields ...Fields)  {}
func (l *NoopLogger) Error(err error, msg string, fields ...Fields) {}
func (l *NoopLogger) Panic(msg string, fields ...Fields)            {}
func (l *NoopLogger) WithFields(fields Fields) Logger {
	return l
}

const ModuleField = "module"

func NewNoopLogger() *NoopLogger {
	return &NoopLogger{}
}

// NewLogger will return the logger on input if not nil, or a noop logger
// otherwise.
func NewLogger(l Logger) Logger {
	if l == nil {
		return &NoopLogger{}
	}
	return l
}

func MergeFields(f1, f2 Fields) Fields {
	allFields := make(Fields, len(f1)+len(f2))
	fieldMaps := []Fields{f1, f2}
	for _, fmap := range fieldMaps {
		for k, v := range fmap {
			allFields[k] = v
		}
	}
	return allFields
}
