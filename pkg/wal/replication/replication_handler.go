// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Handler interface {
	StartReplication(ctx context.Context) error
	ReceiveMessage(ctx context.Context) (Message, error)
	SyncLSN(ctx context.Context, lsn LSN) error
	GetLSNParser() LSNParser
	Close() error
}

type Message interface {
	GetData() *MessageData
}

type MessageData struct {
	LSN            LSN
	Data           []byte
	ServerTime     time.Time
	ReplyRequested bool
}

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
