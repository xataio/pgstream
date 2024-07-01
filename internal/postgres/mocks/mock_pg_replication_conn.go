// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/internal/postgres"
)

type ReplicationConn struct {
	IdentifySystemFn          func(ctx context.Context) (postgres.IdentifySystemResult, error)
	StartReplicationFn        func(ctx context.Context, cfg postgres.ReplicationConfig) error
	SendStandbyStatusUpdateFn func(ctx context.Context, lsn uint64) error
	ReceiveMessageFn          func(ctx context.Context) (*postgres.ReplicationMessage, error)
	CloseFn                   func(ctx context.Context) error
}

func (m *ReplicationConn) IdentifySystem(ctx context.Context) (postgres.IdentifySystemResult, error) {
	return m.IdentifySystemFn(ctx)
}

func (m *ReplicationConn) StartReplication(ctx context.Context, cfg postgres.ReplicationConfig) error {
	return m.StartReplicationFn(ctx, cfg)
}

func (m *ReplicationConn) SendStandbyStatusUpdate(ctx context.Context, lsn uint64) error {
	return m.SendStandbyStatusUpdateFn(ctx, lsn)
}

func (m *ReplicationConn) ReceiveMessage(ctx context.Context) (*postgres.ReplicationMessage, error) {
	return m.ReceiveMessageFn(ctx)
}

func (m *ReplicationConn) Close(ctx context.Context) error {
	return m.CloseFn(ctx)
}
