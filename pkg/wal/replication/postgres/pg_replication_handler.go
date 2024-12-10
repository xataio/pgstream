// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// Handler handles the postgres replication slot operations
type Handler struct {
	logger    loglib.Logger
	logFields loglib.Fields

	pgReplicationConn     pgReplicationConn
	pgReplicationSlotName string
	pgConnBuilder         func() (pglib.Querier, error)

	lsnParser replication.LSNParser
}

type pgReplicationConn interface {
	IdentifySystem(ctx context.Context) (pglib.IdentifySystemResult, error)
	StartReplication(ctx context.Context, cfg pglib.ReplicationConfig) error
	SendStandbyStatusUpdate(ctx context.Context, lsn uint64) error
	ReceiveMessage(ctx context.Context) (*pglib.ReplicationMessage, error)
	Close(ctx context.Context) error
}

type Config struct {
	PostgresURL string
	// Name of the replication slot to listen on. If not provided, it defaults
	// to "pgstream_<dbname>_slot".
	ReplicationSlotName string
}

type Option func(h *Handler)

const (
	logLSNPosition = "position"
	logSlotName    = "slot_name"
	logTimeline    = "timeline"
	logDBName      = "db_name"
	logSystemID    = "system_id"
)

var pluginArguments = []string{
	`"include-timestamp" '1'`,
	`"format-version" '2'`,
	`"write-in-chunks" '1'`,
	`"include-lsn" '1'`,
	`"include-transaction" '0'`,
}

// NewHandler returns a new postgres replication handler for the database on input.
func NewHandler(ctx context.Context, cfg Config, opts ...Option) (*Handler, error) {
	connBuilder := func() (pglib.Querier, error) {
		return pglib.NewConn(ctx, cfg.PostgresURL)
	}

	pgReplicationConn, err := pglib.NewReplicationConn(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	sysID, err := pgReplicationConn.IdentifySystem(ctx)
	if err != nil {
		return nil, fmt.Errorf("identifySystem failed: %w", err)
	}

	replicationSlotName := cfg.ReplicationSlotName
	if replicationSlotName == "" {
		replicationSlotName = pglib.DefaultReplicationSlotName(sysID.DBName)
	}

	h := &Handler{
		logger:                loglib.NewNoopLogger(),
		pgReplicationConn:     pgReplicationConn,
		pgReplicationSlotName: replicationSlotName,
		pgConnBuilder:         connBuilder,
		lsnParser:             &LSNParser{},
		logFields: loglib.Fields{
			logSystemID:    sysID.SystemID,
			logDBName:      sysID.DBName,
			logSlotName:    replicationSlotName,
			logLSNPosition: sysID.XLogPos,
		},
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(h *Handler) {
		h.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_replication_handler",
		})
	}
}

// StartReplication will start the replication process on the configured
// replication slot. It will check for the last synced LSN
// (confirmed_flush_lsn), and if there isn't one, it will start replication from
// the restart_lsn position.
func (h *Handler) StartReplication(ctx context.Context) error {
	conn, err := h.pgConnBuilder()
	if err != nil {
		return fmt.Errorf("creating pg connection: %w", err)
	}
	defer conn.Close(ctx)

	startPos, err := h.getLastSyncedLSN(ctx, conn)
	if err != nil {
		return fmt.Errorf("read last position: %w", err)
	}

	h.logger.Trace("read last LSN position", h.logFields, loglib.Fields{
		logLSNPosition: h.lsnParser.ToString(startPos),
	})

	if startPos == 0 {
		startPos, err = h.getRestartLSN(ctx, conn, h.pgReplicationSlotName)
		if err != nil {
			return fmt.Errorf("get restart LSN: %w", err)
		}
	}

	return h.StartReplicationFromLSN(ctx, startPos)
}

// StartReplicationFromLSN will start the replication process on the configured
// replication slot from the LSN on input.
func (h *Handler) StartReplicationFromLSN(ctx context.Context, lsn replication.LSN) error {
	h.logger.Trace("set start LSN", h.logFields, loglib.Fields{
		logLSNPosition: h.lsnParser.ToString(lsn),
	})

	err := h.pgReplicationConn.StartReplication(
		ctx, pglib.ReplicationConfig{
			SlotName:        h.pgReplicationSlotName,
			StartPos:        uint64(lsn),
			PluginArguments: pluginArguments,
		})
	if err != nil {
		return fmt.Errorf("startReplication: %w", err)
	}

	h.logger.Info("logical replication started", h.logFields)

	return h.SyncLSN(ctx, lsn)
}

// ReceiveMessage will listen for messages from the WAL. It returns an error if
// an unexpected message is received.
func (h *Handler) ReceiveMessage(ctx context.Context) (*replication.Message, error) {
	pgMsg, err := h.pgReplicationConn.ReceiveMessage(ctx)
	if err != nil {
		h.logger.Error(err, "receiving message")
		return nil, mapPostgresError(err)
	}

	return &replication.Message{
		LSN:            replication.LSN(pgMsg.LSN),
		Data:           pgMsg.WALData,
		ServerTime:     pgMsg.ServerTime,
		ReplyRequested: pgMsg.ReplyRequested,
	}, nil
}

// SyncLSN notifies Postgres how far we have processed in the WAL.
func (h *Handler) SyncLSN(ctx context.Context, lsn replication.LSN) error {
	err := h.pgReplicationConn.SendStandbyStatusUpdate(ctx, uint64(lsn))
	if err != nil {
		return fmt.Errorf("syncLSN: send status update: %w", err)
	}
	h.logger.Trace("stored new LSN position", loglib.Fields{
		logLSNPosition: h.lsnParser.ToString(lsn),
	})
	return nil
}

// GetReplicationLag will return the consumer current replication lag. This
// value is different from the postgres replication lag, which takes into
// consideration all consumers and ongoing transactions.
func (h *Handler) GetReplicationLag(ctx context.Context) (int64, error) {
	conn, err := h.pgConnBuilder()
	if err != nil {
		return -1, err
	}
	defer conn.Close(ctx)

	var lag int64
	lagQuery := `SELECT (pg_current_wal_lsn() - confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name=$1`
	if err := conn.QueryRow(ctx, lagQuery, h.pgReplicationSlotName).Scan(&lag); err != nil {
		return -1, err
	}

	return lag, nil
}

func (h *Handler) GetCurrentLSN(ctx context.Context) (replication.LSN, error) {
	conn, err := h.pgConnBuilder()
	if err != nil {
		return 0, fmt.Errorf("creating pg connection: %w", err)
	}
	defer conn.Close(ctx)

	var currentLSN string
	err = conn.QueryRow(ctx, `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=$1`, h.pgReplicationSlotName).Scan(&currentLSN)
	if err != nil {
		return 0, err
	}
	return h.lsnParser.FromString(currentLSN)
}

// GetLSNParser returns a postgres implementation of the LSN parser.
func (h *Handler) GetLSNParser() replication.LSNParser {
	return h.lsnParser
}

// Close closes the database connections.
func (h *Handler) Close() error {
	return h.pgReplicationConn.Close(context.Background())
}

// getRestartLSN returns the absolute earliest possible LSN we can support. If
// the consumer's LSN is earlier than this, we cannot (easily) catch the
// consumer back up.
func (h *Handler) getRestartLSN(ctx context.Context, conn pglib.Querier, slotName string) (replication.LSN, error) {
	var restartLSN string
	err := conn.QueryRow(
		ctx,
		`select restart_lsn from pg_replication_slots where slot_name=$1`,
		slotName,
	).Scan(&restartLSN)
	if err != nil {
		// TODO: improve error message in case the slot doesn't exist
		return 0, err
	}
	return h.lsnParser.FromString(restartLSN)
}

// getLastSyncedLSN gets the `confirmed_flush_lsn` from PG. This is the last LSN
// that the consumer confirmed it had completed.
func (h *Handler) getLastSyncedLSN(ctx context.Context, conn pglib.Querier) (replication.LSN, error) {
	var confirmedFlushLSN string
	err := conn.QueryRow(ctx, `select confirmed_flush_lsn from pg_replication_slots where slot_name=$1`, h.pgReplicationSlotName).Scan(&confirmedFlushLSN)
	if err != nil {
		return 0, err
	}

	return h.lsnParser.FromString(confirmedFlushLSN)
}

func mapPostgresError(err error) error {
	if errors.Is(err, pglib.ErrConnTimeout) {
		return replication.ErrConnTimeout
	}

	// ignore warnings
	replErr := &pglib.Error{}
	if errors.As(err, &replErr) && replErr.Severity == "WARNING" {
		return nil
	}

	return err
}
