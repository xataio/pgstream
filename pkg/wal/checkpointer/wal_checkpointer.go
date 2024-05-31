// SPDX-License-Identifier: Apache-2.0

package checkpointer

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

// Checkpoint defines the way to confirm the positions that have been read.
// The actual implementation depends on the source of events (postgres, kafka,...)
type Checkpoint func(ctx context.Context, positions []wal.CommitPosition) error
