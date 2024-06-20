// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Handler manages the replication operations
type Handler interface {
	StartReplication(ctx context.Context) error
	ReceiveMessage(ctx context.Context) (Message, error)
	SyncLSN(ctx context.Context, lsn LSN) error
	GetReplicationLag(ctx context.Context) (int64, error)
	GetLSNParser() LSNParser
	Close() error
}

type Message interface {
	GetData() *MessageData
}

// MessageData is the common data for all replication messages
type MessageData struct {
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

type Error struct {
	Severity string
	Msg      string
}

func (e *Error) Error() string {
	return fmt.Sprintf("replication error: %s", e.Msg)
}
