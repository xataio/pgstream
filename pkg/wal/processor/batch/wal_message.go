// SPDX-License-Identifier: Apache-2.0

package batch

import "github.com/xataio/pgstream/pkg/wal"

type Message interface {
	Size() int
	IsEmpty() bool
}

// WALMessage is a wrapper around any kind of message implementing the Message
// interface which contains a wal commit position.
type WALMessage[T Message] struct {
	message  T
	position wal.CommitPosition
}

func NewWALMessage[T Message](msg T, pos wal.CommitPosition) *WALMessage[T] {
	return &WALMessage[T]{
		message:  msg,
		position: pos,
	}
}

func (m *WALMessage[T]) GetMessage() T {
	return m.message
}

func (m *WALMessage[T]) GetPosition() wal.CommitPosition {
	return m.position
}

func (m *WALMessage[T]) Size() int {
	return m.message.Size()
}

func (m *WALMessage[T]) isKeepAlive() bool {
	return m.message.IsEmpty() && m.position != ""
}
