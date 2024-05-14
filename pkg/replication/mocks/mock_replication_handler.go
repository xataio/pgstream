package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/replication"
)

type ReplicationHandler struct {
	StartReplicationFn    func(context.Context) error
	ReceiveMessageFn      func(context.Context) (replication.Message, error)
	UpdateLSNPositionFn   func(lsn replication.LSN)
	SyncLSNFn             func(context.Context) error
	DropReplicationSlotFn func(ctx context.Context) error
	GetLSNParserFn        func() replication.LSNParser
	CloseFn               func() error
}

func (m *ReplicationHandler) StartReplication(ctx context.Context) error {
	return m.StartReplicationFn(ctx)
}

func (m *ReplicationHandler) ReceiveMessage(ctx context.Context) (replication.Message, error) {
	return m.ReceiveMessageFn(ctx)
}

func (m *ReplicationHandler) UpdateLSNPosition(lsn replication.LSN) {
	m.UpdateLSNPositionFn(lsn)
}

func (m *ReplicationHandler) SyncLSN(ctx context.Context) error {
	return m.SyncLSNFn(ctx)
}

func (m *ReplicationHandler) DropReplicationSlot(ctx context.Context) error {
	return m.DropReplicationSlotFn(ctx)
}

func (m *ReplicationHandler) GetLSNParser() replication.LSNParser {
	return m.GetLSNParserFn()
}

func (m *ReplicationHandler) Close() error {
	return m.CloseFn()
}
