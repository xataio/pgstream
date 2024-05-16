// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/xataio/pgstream/internal/replication"
)

type Handler struct {
	// Create two connections. One for querying, one for handling replication
	// events.
	pgConn            *pgx.Conn
	pgReplicationConn *pgconn.PgConn

	pgReplicationSlotName string

	// The current (as we know it) position in the WAL.
	currentLSN uint64
	lsnParser  replication.LSNParser
}

const (
	logLSNPosition = "position"
	logSlotName    = "slot_name"
	logTimeline    = "timeline"
	logDBName      = "db_name"
	logSystemID    = "system_id"
)

func NewHandler(ctx context.Context, cfg *pgx.ConnConfig) (*Handler, error) {
	pgConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres client: %w", err)
	}

	// open a second Postgres connection, this one dedicated for replication
	copyConfig := cfg.Copy()
	copyConfig.RuntimeParams["replication"] = "database"

	pgReplicationConn, err := pgconn.ConnectConfig(context.Background(), &copyConfig.Config)
	if err != nil {
		return nil, fmt.Errorf("create postgres replication client: %w", err)
	}

	return &Handler{
		pgConn:            pgConn,
		pgReplicationConn: pgReplicationConn,
		lsnParser:         &LSNParser{},
	}, nil
}

func (h *Handler) StartReplication(ctx context.Context) error {
	sysID, err := pglogrepl.IdentifySystem(ctx, h.pgReplicationConn)
	if err != nil {
		return fmt.Errorf("identifySystem failed: %w", err)
	}

	h.pgReplicationSlotName = fmt.Sprintf("%s_slot", sysID.DBName)

	logger := log.Ctx(ctx).With().
		Str(logSystemID, sysID.SystemID).
		Str(logDBName, sysID.DBName).
		Str(logSlotName, h.pgReplicationSlotName).
		Logger()
	ctx = logger.WithContext(ctx)

	logger.Info().
		Int32(logTimeline, sysID.Timeline).
		Stringer(logLSNPosition, sysID.XLogPos).
		Msg("identifySystem success")

	startPos, err := h.getLastSyncedLSN(ctx)
	if err != nil {
		return fmt.Errorf("read last position: %w", err)
	}

	logger.Trace().
		Stringer(logLSNPosition, pglogrepl.LSN(startPos)).
		Msg("read last LSN position.")

	if startPos == 0 {
		// todo(deverts): If we don't have a position. Read from as early as possible.
		// this _could_ be too old. In the future, it would be good to calculate if we're
		// too far behind, so we can fix it.
		startPos, err = h.getRestartLSN(ctx, h.pgReplicationSlotName)
		if err != nil {
			return fmt.Errorf("get restart LSN: %w", err)
		}
	}

	logger.Trace().Stringer(logLSNPosition, pglogrepl.LSN(startPos)).Msg("set start LSN")

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

	logger.Info().Msgf("logical replication started on slot %v.", h.pgReplicationSlotName)

	h.UpdateLSNPosition(startPos)

	return nil
}

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

func (h *Handler) UpdateLSNPosition(lsn replication.LSN) {
	atomic.StoreUint64(&h.currentLSN, uint64(lsn))
}

// SyncLSN notifies Postgres how far we have processed in the WAL.
func (h *Handler) SyncLSN(ctx context.Context) error {
	lsn := h.getLSNPosition()
	err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		h.pgReplicationConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(lsn)},
	)
	if err != nil {
		return fmt.Errorf("syncLSN: send status update: %w", err)
	}
	log.Ctx(ctx).Trace().Stringer(logLSNPosition, pglogrepl.LSN(lsn)).Msg("stored new LSN position")
	return nil
}

func (h *Handler) DropReplicationSlot(ctx context.Context) error {
	err := pglogrepl.DropReplicationSlot(
		ctx,
		h.pgReplicationConn,
		h.pgReplicationSlotName,
		pglogrepl.DropReplicationSlotOptions{Wait: true},
	)
	if err != nil {
		return fmt.Errorf("clean up replication slot %q: %w", h.pgReplicationSlotName, err)
	}

	return nil
}

func (h *Handler) GetLSNParser() replication.LSNParser {
	return h.lsnParser
}

// Close closes the database connections.
func (h *Handler) Close() error {
	err := h.pgReplicationConn.Close(context.Background())
	if err != nil {
		return err
	}
	return h.pgConn.Close(context.Background())
}

func (h *Handler) getLSNPosition() replication.LSN {
	return replication.LSN(atomic.LoadUint64(&h.currentLSN))
}

// getRestartLSN returns the absolute earliest possible LSN we can support. If
// the consumer's LSN is earlier than this, we cannot (easily) catch the
// consumer back up.
func (h *Handler) getRestartLSN(ctx context.Context, slotName string) (replication.LSN, error) {
	var restartLSN string
	err := h.pgConn.QueryRow(
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
func (h *Handler) getLastSyncedLSN(ctx context.Context) (replication.LSN, error) {
	var confirmedFlushLSN string
	err := h.pgConn.QueryRow(ctx, `select confirmed_flush_lsn from pg_replication_slots where slot_name=$1`, h.pgReplicationSlotName).Scan(&confirmedFlushLSN)
	if err != nil {
		return 0, err
	}

	return h.lsnParser.FromString(confirmedFlushLSN)
}
