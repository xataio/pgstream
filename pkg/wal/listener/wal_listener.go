// SPDX-License-Identifier: Apache-2.0

package listener

import "context"

// Listener represents a process that listens to WAL events.
type Listener interface {
	Listen(ctx context.Context) error
	Close() error
}
