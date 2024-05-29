// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/replication"
	pgreplication "github.com/xataio/pgstream/internal/replication/postgres"

	"github.com/xataio/pgstream/pkg/wal"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

// Listener contains the environment for subscribing and listening to
// postgres logical replication events.
type Listener struct {
	replicationHandler replication.Handler

	// How often we should persist the LSN if not already triggered by the
	// buffer filling up. Default is 5s
	syncInterval time.Duration

	// Function called for processing WAL events.
	processEvent listenerProcessWalEvent

	walDataDeserialiser func([]byte, any) error
}

// listenerProcessWalEvent is the function type callback to process WAL events.
type listenerProcessWalEvent func(context.Context, *wal.Data) error

type Config struct {
	Conn            pgx.ConnConfig
	SyncLSNInterval time.Duration
}

const defaultLSNSyncInterval = time.Second * 5

func NewListener(
	ctx context.Context,
	cfg *Config,
	processEvent listenerProcessWalEvent,
) (*Listener, error) {
	replicationHandler, err := pgreplication.NewHandler(ctx, &cfg.Conn)
	if err != nil {
		return nil, fmt.Errorf("pg listener: create replication handler: %w", err)
	}

	syncInterval := defaultLSNSyncInterval
	if cfg.SyncLSNInterval > 0 {
		syncInterval = cfg.SyncLSNInterval
	}

	l := &Listener{
		replicationHandler:  replicationHandler,
		syncInterval:        syncInterval,
		processEvent:        processEvent,
		walDataDeserialiser: json.Unmarshal,
	}

	return l, nil
}

// Listen starts the subscription process to listen for updates from PG.
func (l *Listener) Listen(ctx context.Context) error {
	if err := l.replicationHandler.StartReplication(ctx); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	return l.listen(ctx)
}

// checkpoint is called by the wal processor. When new records have been pushed
// to postgres or kafka, we update our internal position.
func (l *Listener) Checkpoint(ctx context.Context, positions []wal.CommitPosition) error {
	// we only need the max pg wal offset
	if len(positions) == 0 {
		return nil
	}

	max := positions[0].PGPos
	for _, position := range positions {
		if position.PGPos > max {
			max = position.PGPos
		}
	}

	l.replicationHandler.UpdateLSNPosition(replication.LSN(max))
	return nil
}

// Close closes the listener internal resources
func (l *Listener) Close() error {
	return l.replicationHandler.Close()
}

func (l *Listener) listen(ctx context.Context) error {
	ticker := time.NewTicker(l.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// soft shut-down, save the position
			return l.replicationHandler.SyncLSN(context.WithoutCancel(ctx))
		case <-ticker.C:
			if err := l.replicationHandler.SyncLSN(ctx); err != nil {
				return fmt.Errorf("periodic LSN sync: %w", err)
			}
		default:
			msg, err := l.replicationHandler.ReceiveMessage(ctx)
			if err != nil {
				replErr := &replication.Error{}
				if errors.Is(err, replication.ErrConnTimeout) || (errors.As(err, &replErr) && replErr.Severity == "WARNING") {
					continue
				}
				return fmt.Errorf("receiving message: %w", err)
			}

			msgData := msg.GetData()
			if msgData == nil {
				continue
			}

			switch msgData.Data {
			case nil: // keep alive
				if err := l.processWALKeepalive(ctx, msgData); err != nil {
					return err
				}
			default:
				log.Trace().
					Str("wal_end", l.replicationHandler.GetLSNParser().ToString(msgData.LSN)).
					Time("server_time", msgData.ServerTime).
					Bytes("wal_data", msgData.Data).
					Send()

				if err := l.processWALEvent(ctx, msgData); err != nil {
					if errors.Is(err, context.Canceled) {
						// soft shut-down, save the position
						return l.replicationHandler.SyncLSN(context.Background())
					}
					return err
				}
			}
		}
	}
}

func (l *Listener) processWALKeepalive(ctx context.Context, msgData *replication.MessageData) error {
	l.replicationHandler.UpdateLSNPosition(msgData.LSN)

	if msgData.ReplyRequested {
		if err := l.replicationHandler.SyncLSN(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (l *Listener) processWALEvent(ctx context.Context, msgData *replication.MessageData) error {
	if msgData == nil {
		return nil
	}

	event := &wal.Data{}
	if err := l.walDataDeserialiser(msgData.Data, &event); err != nil {
		return fmt.Errorf("error unmarshaling wal data: %w", err)
	}
	event.CommitPosition = wal.CommitPosition{PGPos: msgData.LSN}

	if err := l.processEvent(ctx, event); err != nil {
		return err
	}

	return nil
}
