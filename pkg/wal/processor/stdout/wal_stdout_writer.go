// SPDX-License-Identifier: Apache-2.0

package stdout

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/xataio/pgstream/internal/json"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
)

// Writer is a processor that serialises WAL events as NDJSON to an
// io.Writer (defaulting to os.Stdout). It is intended for debugging and
// quick validation of a pgstream pipeline without needing a real target.
type Writer struct {
	logger       loglib.Logger
	out          io.Writer
	serialiser   func(any) ([]byte, error)
	checkpointer checkpointer.Checkpoint
}

type Option func(*Writer)

func NewWriter(opts ...Option) *Writer {
	w := &Writer{
		logger:     loglib.NewNoopLogger(),
		out:        os.Stdout,
		serialiser: json.Marshal,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func WithLogger(l loglib.Logger) Option {
	return func(w *Writer) {
		w.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "stdout_writer",
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(w *Writer) {
		w.checkpointer = c
	}
}

// WithWriter overrides the destination io.Writer. Primarily intended for tests.
func WithWriter(out io.Writer) Option {
	return func(w *Writer) {
		w.out = out
	}
}

func (w *Writer) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Panic("[PANIC] Panic while processing replication event", loglib.Fields{
				"wal_data":    walEvent,
				"panic":       r,
				"stack_trace": debug.Stack(),
			})
			retErr = fmt.Errorf("stdout writer: understanding event: %v", r)
		}
	}()

	if walEvent.Data != nil {
		payload, err := w.serialiser(walEvent.Data)
		if err != nil {
			return fmt.Errorf("marshalling event: %w", err)
		}

		if _, err := w.out.Write(append(payload, '\n')); err != nil {
			return fmt.Errorf("writing event: %w", err)
		}
	}

	if w.checkpointer != nil && walEvent.CommitPosition != "" {
		if err := w.checkpointer(ctx, []wal.CommitPosition{walEvent.CommitPosition}); err != nil {
			w.logger.Warn(err, "stdout writer: error updating commit position")
		}
	}

	return nil
}

func (w *Writer) Name() string {
	return "stdout-writer"
}

func (w *Writer) Close() error {
	return nil
}
