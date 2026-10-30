// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestReplicationConn_ReceiveMessage_XLogDataLSNIsTheWALStart(t *testing.T) {
	t.Parallel()

	const walStart = uint64(0x1EC720E)
	walData := []byte(`{"action":"I","schema":"public","table":"test","columns":[{"name":"payload","type":"text","value":"\u0001\u0001\u0001"}]}`)

	conn := newTestReplicationConn(t, xLogData(walStart, walData))

	msg, err := conn.ReceiveMessage(context.Background())
	require.NoError(t, err)
	require.Equal(t, walStart, msg.LSN)
	require.Equal(t, walData, msg.WALData)
}

func newTestReplicationConn(t *testing.T, copyData []byte) *ReplicationConn {
	t.Helper()

	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})

	sent := make(chan error, 1)
	go func() {
		backend := pgproto3.NewBackend(server, server)
		backend.Send(&pgproto3.CopyData{Data: copyData})
		sent <- backend.Flush()
	}()
	t.Cleanup(func() { require.NoError(t, <-sent) })

	cfg, err := ParseConfig(testConnURL)
	require.NoError(t, err)

	pgConn, err := pgconn.Construct(&pgconn.HijackedConn{Conn: client, Config: &cfg.Config})
	require.NoError(t, err)

	return &ReplicationConn{conn: pgConn}
}

func xLogData(walStart uint64, walData []byte) []byte {
	msg := []byte{pglogrepl.XLogDataByteID}
	msg = binary.BigEndian.AppendUint64(msg, walStart)
	msg = binary.BigEndian.AppendUint64(msg, walStart)
	msg = binary.BigEndian.AppendUint64(msg, 0)
	return append(msg, walData...)
}
