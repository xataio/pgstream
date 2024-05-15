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

type ReplicationHandler struct {
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

func NewReplicationHandler(ctx context.Context, cfg *pgx.ConnConfig) (*ReplicationHandler, error) {
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

	return &ReplicationHandler{
		pgConn:            pgConn,
		pgReplicationConn: pgReplicationConn,
		lsnParser:         &LSNParser{},
	}, nil
}

func (c *ReplicationHandler) StartReplication(ctx context.Context) error {
	sysID, err := pglogrepl.IdentifySystem(ctx, c.pgReplicationConn)
	if err != nil {
		return fmt.Errorf("identifySystem failed: %w", err)
	}

	c.pgReplicationSlotName = fmt.Sprintf("%s_slot", sysID.DBName)

	logger := log.Ctx(ctx).With().
		Str(logSystemID, sysID.SystemID).
		Str(logDBName, sysID.DBName).
		Str(logSlotName, c.pgReplicationSlotName).
		Logger()
	ctx = logger.WithContext(ctx)

	logger.Info().
		Int32(logTimeline, sysID.Timeline).
		Stringer(logLSNPosition, sysID.XLogPos).
		Msg("identifySystem success")

	startPos, err := c.getLastSyncedLSN(ctx)
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
		startPos, err = c.getRestartLSN(ctx, c.pgReplicationSlotName)
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
		c.pgReplicationConn,
		c.pgReplicationSlotName,
		pglogrepl.LSN(startPos),
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("startReplication: %w", err)
	}

	logger.Info().Msgf("logical replication started on slot %v.", c.pgReplicationSlotName)

	c.UpdateLSNPosition(startPos)

	return nil
}

func (c *ReplicationHandler) ReceiveMessage(ctx context.Context) (replication.Message, error) {
	msg, err := c.pgReplicationConn.ReceiveMessage(ctx)
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

func (c *ReplicationHandler) UpdateLSNPosition(lsn replication.LSN) {
	atomic.StoreUint64(&c.currentLSN, uint64(lsn))
}

// SyncLSN notifies Postgres how far we have processed in the WAL.
func (c *ReplicationHandler) SyncLSN(ctx context.Context) error {
	lsn := c.getLSNPosition()
	err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		c.pgReplicationConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(lsn)},
	)
	if err != nil {
		return fmt.Errorf("syncLSN: send status update: %w", err)
	}
	log.Ctx(ctx).Trace().Stringer(logLSNPosition, pglogrepl.LSN(lsn)).Msg("stored new LSN position")
	return nil
}

func (c *ReplicationHandler) DropReplicationSlot(ctx context.Context) error {
	err := pglogrepl.DropReplicationSlot(
		ctx,
		c.pgReplicationConn,
		c.pgReplicationSlotName,
		pglogrepl.DropReplicationSlotOptions{Wait: true},
	)
	if err != nil {
		return fmt.Errorf("clean up replication slot %q: %w", c.pgReplicationSlotName, err)
	}

	return nil
}

func (c *ReplicationHandler) GetLSNParser() replication.LSNParser {
	return c.lsnParser
}

// Close closes the database connections.
func (c *ReplicationHandler) Close() error {
	err := c.pgReplicationConn.Close(context.Background())
	if err != nil {
		return err
	}
	return c.pgConn.Close(context.Background())
}

func (c *ReplicationHandler) getLSNPosition() replication.LSN {
	return replication.LSN(atomic.LoadUint64(&c.currentLSN))
}

// getRestartLSN returns the absolute earliest possible LSN we can support. If
// the consumer's LSN is earlier than this, we cannot (easily) catch the
// consumer back up.
func (c *ReplicationHandler) getRestartLSN(ctx context.Context, slotName string) (replication.LSN, error) {
	var restartLSN string
	err := c.pgConn.QueryRow(
		ctx,
		`select restart_lsn from pg_replication_slots where slot_name=$1`,
		slotName,
	).Scan(&restartLSN)
	if err != nil {
		// TODO: improve error message in case the slot doesn't exist
		return 0, err
	}
	return c.lsnParser.FromString(restartLSN)
}

// getLastSyncedLSN gets the `confirmed_flush_lsn` from PG. This is the last LSN
// that the consumer confirmed it had completed.
func (c *ReplicationHandler) getLastSyncedLSN(ctx context.Context) (replication.LSN, error) {
	var confirmedFlushLSN string
	err := c.pgConn.QueryRow(ctx, `select confirmed_flush_lsn from pg_replication_slots where slot_name=$1`, c.pgReplicationSlotName).Scan(&confirmedFlushLSN)
	if err != nil {
		return 0, err
	}

	return c.lsnParser.FromString(confirmedFlushLSN)
}
