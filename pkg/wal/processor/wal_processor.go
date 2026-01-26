// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/pkg/wal"
)

// Processor is a general interface to receive and process a wal event
type Processor interface {
	ProcessWALEvent(ctx context.Context, walEvent *wal.Event) error
	Close() error
	Name() string
}

var ErrPanic = errors.New("panic while processing wal event")
