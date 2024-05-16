// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

type Processor interface {
	ProcessWALEvent(ctx context.Context, walEvent *wal.Data) error
	Close() error
}
