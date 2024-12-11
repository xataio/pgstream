// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Generator struct {
	CreateSnapshotFn func(ctx context.Context, snapshot *snapshot.Snapshot) error
	CloseFn          func() error
}

func (m *Generator) CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) error {
	return m.CreateSnapshotFn(ctx, snapshot)
}

func (m *Generator) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
