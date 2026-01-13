// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"sync/atomic"

	"github.com/xataio/pgstream/pkg/wal/replication"
)

type Handler struct {
	StartReplicationFn        func(context.Context) error
	StartReplicationFromLSNFn func(context.Context, replication.LSN) error
	ReceiveMessageFn          func(context.Context, uint64) (*replication.Message, error)
	SyncLSNFn                 func(context.Context, replication.LSN, uint64) error
	DropReplicationSlotFn     func(ctx context.Context) error
	GetLSNParserFn            func() replication.LSNParser
	GetCurrentLSNFn           func(context.Context) (replication.LSN, error)
	ResetConnectionFn         func(ctx context.Context) error
	GetReplicationLagFn       func(context.Context) (int64, error)
	GetReplicationSlotNameFn  func() string
	CloseFn                   func() error
	SyncLSNCalls              uint64
	ReceiveMessageCalls       uint64
}

func (m *Handler) StartReplication(ctx context.Context) error {
	return m.StartReplicationFn(ctx)
}

func (m *Handler) StartReplicationFromLSN(ctx context.Context, lsn replication.LSN) error {
	return m.StartReplicationFromLSNFn(ctx, lsn)
}

func (m *Handler) ReceiveMessage(ctx context.Context) (*replication.Message, error) {
	atomic.AddUint64(&m.ReceiveMessageCalls, 1)
	return m.ReceiveMessageFn(ctx, m.GetReceiveMessageCalls())
}

func (m *Handler) SyncLSN(ctx context.Context, lsn replication.LSN) error {
	atomic.AddUint64(&m.SyncLSNCalls, 1)
	return m.SyncLSNFn(ctx, lsn, m.GetSyncLSNCalls())
}

func (m *Handler) DropReplicationSlot(ctx context.Context) error {
	return m.DropReplicationSlotFn(ctx)
}

func (m *Handler) GetCurrentLSN(ctx context.Context) (replication.LSN, error) {
	return m.GetCurrentLSNFn(ctx)
}

func (m *Handler) GetReplicationLag(ctx context.Context) (int64, error) {
	return m.GetReplicationLagFn(ctx)
}

func (m *Handler) GetLSNParser() replication.LSNParser {
	return m.GetLSNParserFn()
}

func (m *Handler) Close() error {
	return m.CloseFn()
}

func (m *Handler) GetSyncLSNCalls() uint64 {
	return atomic.LoadUint64(&m.SyncLSNCalls)
}

func (m *Handler) GetReceiveMessageCalls() uint64 {
	return atomic.LoadUint64(&m.ReceiveMessageCalls)
}

func (m *Handler) ResetConnection(ctx context.Context) error {
	return m.ResetConnectionFn(ctx)
}

func (m *Handler) GetReplicationSlotName() string {
	if m.GetReplicationSlotNameFn == nil {
		return "mock_slot"
	}
	return m.GetReplicationSlotNameFn()
}
