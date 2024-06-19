// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"github.com/jackc/pglogrepl"

	"github.com/xataio/pgstream/pkg/wal/replication"
)

// PrimaryKeepAliveMessage contains no wal data and a flag to indicate if a
// response is requested along with the message metadata (lsn and server time).
type PrimaryKeepAliveMessage pglogrepl.PrimaryKeepaliveMessage

func (pka *PrimaryKeepAliveMessage) GetData() *replication.MessageData {
	return &replication.MessageData{
		LSN:            replication.LSN(pka.ServerWALEnd),
		ServerTime:     pka.ServerTime,
		ReplyRequested: pka.ReplyRequested,
	}
}

// XLogDataMessage contains the wal data along with the message metadata (lsn
// and server time)
type XLogDataMessage pglogrepl.XLogData

func (xld *XLogDataMessage) GetData() *replication.MessageData {
	newLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	return &replication.MessageData{
		LSN:            replication.LSN(newLSN),
		ServerTime:     xld.ServerTime,
		ReplyRequested: false,
		Data:           xld.WALData,
	}
}
