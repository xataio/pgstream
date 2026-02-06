// SPDX-License-Identifier: Apache-2.0

package natsjetstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	natslib "github.com/xataio/pgstream/pkg/nats"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// Reader is a NATS JetStream reader that listens to wal events.
type Reader struct {
	reader       natsReader
	unmarshaler  func([]byte, any) error
	logger       loglib.Logger
	offsetParser natslib.OffsetParser

	// processRecord is called for a new record.
	processRecord payloadProcessor
}

type natsReader interface {
	FetchMessage(context.Context) (*natslib.Message, error)
}

type payloadProcessor func(context.Context, *wal.Event) error

type Option func(*Reader)

// NewWALReader returns a NATS JetStream reader that listens to wal events and
// calls the processor on input.
func NewWALReader(natsReader natsReader, processRecord payloadProcessor, opts ...Option) (*Reader, error) {
	r := &Reader{
		logger:        loglib.NewNoopLogger(),
		processRecord: processRecord,
		unmarshaler:   json.Unmarshal,
		offsetParser:  natslib.NewOffsetParser(),
		reader:        natsReader,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

func WithLogger(logger loglib.Logger) Option {
	return func(r *Reader) {
		r.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_nats_jetstream_reader",
		})
	}
}

func (r *Reader) Listen(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := r.reader.FetchMessage(ctx)
			if err != nil {
				return fmt.Errorf("reading from nats jetstream: %w", err)
			}

			r.logger.Trace("received", loglib.Fields{
				"subject":  msg.Subject,
				"wal_data": msg.Value,
			})

			event := &wal.Event{
				CommitPosition: wal.CommitPosition(r.offsetParser.ToString(&natslib.Offset{
					Stream:    msg.Subject,
					Consumer:  "",
					StreamSeq: 0,
				})),
			}
			event.Data = &wal.Data{}
			if err := r.unmarshaler(msg.Value, event.Data); err != nil {
				return fmt.Errorf("error unmarshaling message value into wal data: %w", err)
			}

			if err = r.processRecord(ctx, event); err != nil {
				if errors.Is(err, context.Canceled) {
					return fmt.Errorf("canceled: %w", err)
				}

				r.logger.Error(err, "processing nats jetstream msg", loglib.Fields{
					"severity": "DATALOSS",
					"wal_data": msg.Value,
				})
			}
		}
	}
}

func (r *Reader) Close() error {
	return nil
}
