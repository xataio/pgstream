// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"errors"
	"time"
)

// Handler manages the replication operations
type Handler interface {
	StartReplication(ctx context.Context) error
	StartReplicationFromLSN(ctx context.Context, lsn LSN) error
	ReceiveMessage(ctx context.Context) (*Message, error)
	SyncLSN(ctx context.Context, lsn LSN) error
	GetReplicationLag(ctx context.Context) (int64, error)
	GetCurrentLSN(ctx context.Context) (LSN, error)
	ResetConnection(ctx context.Context) error
	GetLSNParser() LSNParser
	GetReplicationSlotName() string
	Close() error
}

// Message contains the replication data
type Message struct {
	LSN            LSN
	Data           []byte
	ServerTime     time.Time
	ReplyRequested bool
}

// LSNParser handles the LSN type conversion
type LSNParser interface {
	ToString(LSN) string
	FromString(string) (LSN, error)
}

type LSN uint64

var ErrConnTimeout = errors.New("connection timeout")
