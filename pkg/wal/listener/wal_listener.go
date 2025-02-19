// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

// Listener represents a process that listens to WAL events.
type Listener interface {
	Listen(ctx context.Context) error
	Close() error
}

type ProcessWalEvent func(context.Context, *wal.Event) error
