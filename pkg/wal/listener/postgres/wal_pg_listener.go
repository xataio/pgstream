// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/internal/phase"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// Listener contains the environment for subscribing and listening to
// postgres logical replication events.
type Listener struct {
	replicationHandler replicationHandler
	logger             loglib.Logger
	lsnParser          replication.LSNParser
	snapshotGenerator  snapshotGenerator
	phaseTracker       *phase.Tracker

	// Function called for processing WAL events.
	processEvent listenerProcessWalEvent

	walDataDeserialiser func([]byte, any) error

	nextLocalTxID   uint64
	activeLocalTxID uint64
	generation      uint64
}

type replicationHandler interface {
	StartReplication(ctx context.Context) error
	StartReplicationFromLSN(ctx context.Context, lsn replication.LSN) error
	ReceiveMessage(ctx context.Context) (*replication.Message, error)
	GetCurrentLSN(ctx context.Context) (replication.LSN, error)
	GetLSNParser() replication.LSNParser
	Close() error
}

type snapshotGenerator interface {
	CreateSnapshot(context.Context) error
}

// listenerProcessWalEvent is the function type callback to process WAL events.
type listenerProcessWalEvent func(context.Context, *wal.Event) error

type Option func(l *Listener)

const pgstreamSchemaName = "pgstream"

func New(handler replicationHandler, processEvent listenerProcessWalEvent, opts ...Option) *Listener {
	l := &Listener{
		logger:              loglib.NewNoopLogger(),
		replicationHandler:  handler,
		processEvent:        processEvent,
		walDataDeserialiser: json.UnmarshalUseInt64,
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
			loglib.ModuleField: "wal_postgres_listener",
		})
	}
}

func WithInitialSnapshot(sg snapshotGenerator) Option {
	return func(l *Listener) {
		l.snapshotGenerator = sg
	}
}

// WithPhaseTracker registers a tracker updated as the listener moves between
// snapshot and replication phases.
func WithPhaseTracker(t *phase.Tracker) Option {
	return func(l *Listener) {
		l.phaseTracker = t
	}
}

// Listen starts the subscription process to listen for updates from PG.
func (l *Listener) Listen(ctx context.Context) error {
	if l.snapshotGenerator != nil {
		if err := l.snapshotAndListen(ctx); err != nil {
			l.logger.Error(err, "pg snapshot and listen")
			return err
		}
		// snapshotAndListen already entered the listen loop; only reached on
		// clean return (context cancel) or if snapshot path returned early.
		return nil
	}

	if err := l.replicationHandler.StartReplication(ctx); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	l.phaseTracker.Set(phase.Replication)

	return l.listen(ctx)
}

// Close closes the listener internal resources
func (l *Listener) Close() error {
	return nil
}

func (l *Listener) snapshotAndListen(ctx context.Context) error {
	l.phaseTracker.Set(phase.Snapshot)

	lsn, err := l.replicationHandler.GetCurrentLSN(ctx)
	if err != nil {
		return err
	}

	if err := l.snapshotGenerator.CreateSnapshot(ctx); err != nil {
		return err
	}

	if err := l.replicationHandler.StartReplicationFromLSN(ctx, lsn); err != nil {
		return fmt.Errorf("start replication from LSN %s: %w", l.lsnParser.ToString(lsn), err)
	}
	l.phaseTracker.Set(phase.Replication)

	return l.listen(ctx)
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

			if l.logger.IsTraceEnabled() {
				l.logger.Trace("", loglib.Fields{
					"wal_end":     l.lsnParser.ToString(msg.LSN),
					"server_time": msg.ServerTime,
					"wal_data":    msg.Data,
				})
			}

			if err := l.processWALEvent(ctx, msg); err != nil {
				return err
			}
		}
	}
}

func (l *Listener) processWALEvent(ctx context.Context, msg *replication.Message) error {
	l.startGeneration(msg.Generation)

	// if there's no data, it's a keep alive. If a reply is not requested,
	// no need to process this message.
	if msg.Data == nil && !msg.ReplyRequested {
		return nil
	}

	event := &wal.Event{Generation: msg.Generation}
	if msg.Data != nil {
		event.Data = &wal.Data{}
		if err := l.walDataDeserialiser(msg.Data, event.Data); err != nil {
			return fmt.Errorf("error unmarshaling wal data: %w", err)
		}
		// Here rather than in the writer: this is the only place that knows the
		// value came from wal2json, so one pass covers every downstream call
		// site, and transformers then see the same []byte the snapshot path
		// gives them instead of hex text.
		decodeByteaColumns(event.Data)
	}
	event.CommitPosition = wal.CommitPosition(l.lsnParser.ToString(msg.LSN))
	if err := l.addTransactionMetadata(event); err != nil {
		return err
	}
	if isInternalPgstreamDML(event.Data) {
		l.logger.Trace("skipping pgstream internal DML event", loglib.Fields{
			"schema": event.Data.Schema,
			"table":  event.Data.Table,
			"action": event.Data.Action,
		})
		event.Data = nil
	}

	return l.processEvent(ctx, event)
}

func (l *Listener) addTransactionMetadata(event *wal.Event) error {
	if event.Data == nil {
		return nil
	}

	switch event.Data.Action {
	case "B":
		if l.activeLocalTxID != 0 {
			return fmt.Errorf("received transaction begin while transaction %d is active", l.activeLocalTxID)
		}
		l.nextLocalTxID++
		l.activeLocalTxID = l.nextLocalTxID

		event.Transaction = &wal.Transaction{
			ID:       l.activeLocalTxID,
			Boundary: wal.TransactionBegin,
		}
		// Transaction boundaries are control records, they don't have data.
		event.Data = nil
	case "C":
		if l.activeLocalTxID == 0 {
			return errors.New("received transaction commit without an active transaction")
		}
		event.Transaction = &wal.Transaction{
			ID:       l.activeLocalTxID,
			Boundary: wal.TransactionCommit,
		}
		l.activeLocalTxID = 0
		// Transaction boundaries are control records, they don't have data.
		event.Data = nil
	default:
		if l.activeLocalTxID != 0 {
			event.Transaction = &wal.Transaction{
				ID:       l.activeLocalTxID,
				Boundary: wal.TransactionEntry,
			}
		}
	}

	return nil
}

// startGeneration resets listener-local transaction metadata when the
// replication connection changes. It is called only by the listener goroutine,
// so reconnects cannot race with transaction parsing.
func (l *Listener) startGeneration(generation uint64) {
	if generation == l.generation {
		return
	}

	l.generation = generation
	l.nextLocalTxID = 0
	l.activeLocalTxID = 0
}

func isInternalPgstreamDML(data *wal.Data) bool {
	return data != nil && data.Schema == pgstreamSchemaName && !data.IsDDLEvent()
}
