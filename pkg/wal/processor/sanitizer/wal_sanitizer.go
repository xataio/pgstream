// SPDX-License-Identifier: Apache-2.0

package sanitizer

import (
	"context"
	"fmt"
	"strings"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Sanitizer is a decorator around a wal processor that sanitizes wal event
// column values before passing them to the inner processor. It strips null
// bytes (0x00) from string values, which PostgreSQL does not allow in
// text/varchar columns. Source databases may contain them due to historical
// bugs in older PostgreSQL versions where the binary receive function did not
// validate for null bytes.
type Sanitizer struct {
	logger    loglib.Logger
	processor processor.Processor
}

type Option func(*Sanitizer)

func WithLogger(l loglib.Logger) Option {
	return func(s *Sanitizer) {
		s.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_sanitizer",
		})
	}
}

func New(inner processor.Processor, opts ...Option) *Sanitizer {
	s := &Sanitizer{
		logger:    loglib.NewNoopLogger(),
		processor: inner,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Sanitizer) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if event.Data != nil {
		s.sanitizeColumns(event.Data)
	}
	return s.processor.ProcessWALEvent(ctx, event)
}

func (s *Sanitizer) Name() string {
	return s.processor.Name()
}

func (s *Sanitizer) Close() error {
	return s.processor.Close()
}

func (s *Sanitizer) sanitizeColumns(data *wal.Data) {
	for i, col := range data.Columns {
		if str, ok := col.Value.(string); ok && strings.ContainsRune(str, 0) {
			data.Columns[i].Value = strings.ReplaceAll(str, "\x00", "")
			fields := loglib.Fields{
				"schema": data.Schema,
				"table":  data.Table,
				"column": col.Name,
			}
			if pk := identityValues(data); pk != "" {
				fields["identity"] = pk
			}
			s.logger.Warn(nil, "stripped null bytes from column value", fields)
		}
	}
}

// identityValues returns a string representation of the row's identity
// (primary key) column values for logging purposes.
func identityValues(data *wal.Data) string {
	ids := data.Identity
	if len(ids) == 0 {
		return ""
	}
	if len(ids) == 1 {
		return fmt.Sprintf("%v", ids[0].Value)
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%s=%v", id.Name, id.Value)
	}
	return strings.Join(parts, ",")
}
