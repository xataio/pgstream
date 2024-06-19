// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// Handler handles the postgres replication slot operations
type Handler struct {
	logger loglib.Logger

	pgReplicationConn     *pgconn.PgConn
	pgReplicationSlotName string
	pgConnBuilder         func() (*pgx.Conn, error)

	lsnParser replication.LSNParser
}

type Config struct {
	PostgresURL string
}

type Option func(h *Handler)

const (
	logLSNPosition = "position"
	logSlotName    = "slot_name"
	logTimeline    = "timeline"
	logDBName      = "db_name"
	logSystemID    = "system_id"
)

// NewHandler returns a new postgres replication handler for the database on input.
func NewHandler(ctx context.Context, cfg Config, opts ...Option) (*Handler, error) {
	pgCfg, err := pgx.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", err)
	}

	connBuilder := func() (*pgx.Conn, error) {
		return pgx.ConnectConfig(ctx, pgCfg)
	}

	// open a Postgres connection dedicated for replication
	copyConfig := pgCfg.Copy()
	copyConfig.RuntimeParams["replication"] = "database"

	pgReplicationConn, err := pgconn.ConnectConfig(context.Background(), &copyConfig.Config)
	if err != nil {
		return nil, fmt.Errorf("create postgres replication client: %w", err)
	}

	h := &Handler{
		logger:            loglib.NewNoopLogger(),
		pgReplicationConn: pgReplicationConn,
		pgConnBuilder:     connBuilder,
		lsnParser:         &LSNParser{},
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(h *Handler) {
		h.logger = loglib.NewLogger(l)
	}
}

// StartReplication will start the replication process on the configured
// replication slot. It will check for the last synced LSN
// (confirmed_flush_lsn), and if there isn't one, it will start replication from
// the restart_lsn position.
func (h *Handler) StartReplication(ctx context.Context) error {
	sysID, err := pglogrepl.IdentifySystem(ctx, h.pgReplicationConn)
	if err != nil {
		return fmt.Errorf("identifySystem failed: %w", err)
	}

	h.pgReplicationSlotName = fmt.Sprintf("pgstream_%s_slot", sysID.DBName)

	logFields := loglib.Fields{
		logSystemID: sysID.SystemID,
		logDBName:   sysID.DBName,
		logSlotName: h.pgReplicationSlotName,
	}
	h.logger.Info("replication handler: identifySystem success", logFields, loglib.Fields{
		logTimeline:    sysID.Timeline,
		logLSNPosition: sysID.XLogPos,
	})

	conn, err := h.pgConnBuilder()
	if err != nil {
		return fmt.Errorf("creating pg connection: %w", err)
	}
	defer conn.Close(ctx)

	startPos, err := h.getLastSyncedLSN(ctx, conn)
	if err != nil {
		return fmt.Errorf("read last position: %w", err)
	}

	h.logger.Trace("replication handler: read last LSN position", logFields, loglib.Fields{
		logLSNPosition: pglogrepl.LSN(startPos),
	})

	if startPos == 0 {
		startPos, err = h.getRestartLSN(ctx, conn, h.pgReplicationSlotName)
		if err != nil {
			return fmt.Errorf("get restart LSN: %w", err)
		}
	}

	h.logger.Trace("replication handler: set start LSN", logFields, loglib.Fields{
		logLSNPosition: pglogrepl.LSN(startPos),
	})

	pluginArguments := []string{
		`"include-timestamp" '1'`,
		`"format-version" '2'`,
		`"write-in-chunks" '1'`,
		`"include-lsn" '1'`,
		`"include-transaction" '0'`,
	}
	err = pglogrepl.StartReplication(
		ctx,
		h.pgReplicationConn,
		h.pgReplicationSlotName,
		pglogrepl.LSN(startPos),
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("startReplication: %w", err)
	}

	h.logger.Info("replication handler: logical replication started", logFields)

	return h.SyncLSN(ctx, startPos)
}

// ReceiveMessage will listen for messages from the WAL. It returns an error if
// an unexpected message is received.
func (h *Handler) ReceiveMessage(ctx context.Context) (replication.Message, error) {
	msg, err := h.pgReplicationConn.ReceiveMessage(ctx)
	if err != nil {
		return nil, mapPostgresError(err)
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("parse keep alive: %w", err)
			}
			pkaMessage := PrimaryKeepAliveMessage(pka)
			return &pkaMessage, nil
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("parse xlog data: %w", err)
			}

			xldMessage := XLogDataMessage(xld)
			return &xldMessage, nil
		default:
			return nil, fmt.Errorf("%v: %w", msg.Data[0], ErrUnsupportedCopyDataMessage)
		}
	case *pgproto3.NoticeResponse:
		return nil, parseErrNoticeResponse(msg)
	default:
		// unexpected message (WAL error?)
		return nil, fmt.Errorf("unexpected message: %#v", msg)
	}
}

// SyncLSN notifies Postgres how far we have processed in the WAL.
func (h *Handler) SyncLSN(ctx context.Context, lsn replication.LSN) error {
	err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		h.pgReplicationConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(lsn)},
	)
	if err != nil {
		return fmt.Errorf("syncLSN: send status update: %w", err)
	}
	h.logger.Trace("stored new LSN position", loglib.Fields{
		logLSNPosition: pglogrepl.LSN(lsn).String(),
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
func (h *Handler) getRestartLSN(ctx context.Context, conn *pgx.Conn, slotName string) (replication.LSN, error) {
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
func (h *Handler) getLastSyncedLSN(ctx context.Context, conn *pgx.Conn) (replication.LSN, error) {
	var confirmedFlushLSN string
	err := conn.QueryRow(ctx, `select confirmed_flush_lsn from pg_replication_slots where slot_name=$1`, h.pgReplicationSlotName).Scan(&confirmedFlushLSN)
	if err != nil {
		return 0, err
	}

	return h.lsnParser.FromString(confirmedFlushLSN)
}
