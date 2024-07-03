// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// Listener contains the environment for subscribing and listening to
// postgres logical replication events.
type Listener struct {
	replicationHandler replicationHandler
	logger             loglib.Logger
	lsnParser          replication.LSNParser

	// Function called for processing WAL events.
	processEvent listenerProcessWalEvent

	walDataDeserialiser func([]byte, any) error
}

type replicationHandler interface {
	StartReplication(ctx context.Context) error
	ReceiveMessage(ctx context.Context) (*replication.Message, error)
	GetLSNParser() replication.LSNParser
	Close() error
}

// listenerProcessWalEvent is the function type callback to process WAL events.
type listenerProcessWalEvent func(context.Context, *wal.Event) error

type Config struct {
	Replication pgreplication.Config
}

type Option func(l *Listener)

func New(handler replicationHandler, processEvent listenerProcessWalEvent, opts ...Option) *Listener {
	l := &Listener{
		logger:              loglib.NewNoopLogger(),
		replicationHandler:  handler,
		processEvent:        processEvent,
		walDataDeserialiser: json.Unmarshal,
		lsnParser:           handler.GetLSNParser(),
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

func WithLogger(logger loglib.Logger) Option {
	return func(l *Listener) {
		l.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ServiceField: "wal_postgres_listener",
		})
	}
}

// Listen starts the subscription process to listen for updates from PG.
func (l *Listener) Listen(ctx context.Context) error {
	if err := l.replicationHandler.StartReplication(ctx); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	return l.listen(ctx)
}

// Close closes the listener internal resources
func (l *Listener) Close() error {
	return l.replicationHandler.Close()
}

func (l *Listener) listen(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := l.replicationHandler.ReceiveMessage(ctx)
			if err != nil {
				if errors.Is(err, replication.ErrConnTimeout) {
					continue
				}
				return fmt.Errorf("receiving message: %w", err)
			}

			if msg == nil {
				continue
			}

			l.logger.Trace("", loglib.Fields{
				"wal_end":     l.lsnParser.ToString(msg.LSN),
				"server_time": msg.ServerTime,
				"wal_data":    msg.Data,
			})

			if err := l.processWALEvent(ctx, msg); err != nil {
				return err
			}
		}
	}
}

func (l *Listener) processWALEvent(ctx context.Context, msg *replication.Message) error {
	// if there's no data, it's a keep alive. If a reply is not requested,
	// no need to process this message.
	if msg.Data == nil && !msg.ReplyRequested {
		return nil
	}

	event := &wal.Event{}
	if msg.Data != nil {
		event.Data = &wal.Data{}
		if err := l.walDataDeserialiser(msg.Data, event.Data); err != nil {
			return fmt.Errorf("error unmarshaling wal data: %w", err)
		}
	}
	event.CommitPosition = wal.CommitPosition(l.lsnParser.ToString(msg.LSN))

	return l.processEvent(ctx, event)
}
