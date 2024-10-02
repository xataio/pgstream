// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Generator interface {
	CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) error
	Close(ctx context.Context) error
}
