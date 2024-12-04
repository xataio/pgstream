// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type SnapshotGenerator interface {
	CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) error
	Close() error
}
