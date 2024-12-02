// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type ReplicationConn struct {
	conn *pgconn.PgConn
}

type ReplicationConfig struct {
	SlotName        string
	StartPos        uint64
	PluginArguments []string
}

type ReplicationMessage struct {
	LSN            uint64
	ServerTime     time.Time
	WALData        []byte
	ReplyRequested bool
}

type IdentifySystemResult pglogrepl.IdentifySystemResult

var ErrUnsupportedCopyDataMessage = errors.New("unsupported copy data message")

func NewReplicationConn(ctx context.Context, url string) (*ReplicationConn, error) {
	pgCfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", err)
	}

	pgCfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(context.Background(), &pgCfg.Config)
	if err != nil {
		return nil, fmt.Errorf("create postgres replication client: %w", mapError(err))
	}

	return &ReplicationConn{
		conn: conn,
	}, nil
}

func (c *ReplicationConn) IdentifySystem(ctx context.Context) (IdentifySystemResult, error) {
	res, err := pglogrepl.IdentifySystem(ctx, c.conn)
	return IdentifySystemResult(res), mapError(err)
}

func (c *ReplicationConn) StartReplication(ctx context.Context, cfg ReplicationConfig) error {
	return mapError(pglogrepl.StartReplication(
		ctx,
		c.conn,
		cfg.SlotName,
		pglogrepl.LSN(cfg.StartPos),
		pglogrepl.StartReplicationOptions{PluginArgs: cfg.PluginArguments}))
}

func (c *ReplicationConn) SendStandbyStatusUpdate(ctx context.Context, lsn uint64) error {
	return mapError(pglogrepl.SendStandbyStatusUpdate(
		ctx,
		c.conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(lsn)},
	))
}

func (c *ReplicationConn) ReceiveMessage(ctx context.Context) (*ReplicationMessage, error) {
	msg, err := c.conn.ReceiveMessage(ctx)
	if err != nil {
		return nil, mapError(err)
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("parse keep alive: %w", err)
			}
			return &ReplicationMessage{
				LSN:            uint64(pka.ServerWALEnd),
				ServerTime:     pka.ServerTime,
				ReplyRequested: pka.ReplyRequested,
			}, nil
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("parse xlog data: %w", err)
			}

			return &ReplicationMessage{
				LSN:        uint64(xld.WALStart) + uint64(len(xld.WALData)),
				ServerTime: xld.ServerTime,
				WALData:    xld.WALData,
			}, nil
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

func (c *ReplicationConn) Close(ctx context.Context) error {
	return mapError(c.conn.Close(ctx))
}

func DefaultReplicationSlotName(dbName string) string {
	// sanitise the dbName before creating the replication slot name to ensure
	// the name does not contain invalid characters.
	dbName = strings.ReplaceAll(dbName, ".", "_")
	return "pgstream_" + dbName + "_slot"
}

type Error struct {
	Severity string
	Msg      string
}

func (e *Error) Error() string {
	return fmt.Sprintf("replication error: %s", e.Msg)
}

func parseErrNoticeResponse(errMsg *pgproto3.NoticeResponse) error {
	return &Error{
		Severity: errMsg.Severity,
		Msg: fmt.Sprintf("replication notice response: severity: %s, code: %s, message: %s, detail: %s, schemaName: %s, tableName: %s, columnName: %s",
			errMsg.Severity, errMsg.Code, errMsg.Message, errMsg.Detail, errMsg.SchemaName, errMsg.TableName, errMsg.ColumnName),
	}
}
