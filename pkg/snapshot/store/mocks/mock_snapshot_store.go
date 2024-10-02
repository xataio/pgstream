// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Store struct {
	CreateSnapshotRequestFn func(context.Context, *snapshot.Snapshot) error
	UpdateSnapshotRequestFn func(context.Context, *snapshot.Snapshot) error
	GetSnapshotRequestsFn   func(ctx context.Context, status snapshot.Status) ([]*snapshot.Snapshot, error)
}

func (m *Store) CreateSnapshotRequest(ctx context.Context, s *snapshot.Snapshot) error {
	return m.CreateSnapshotRequestFn(ctx, s)
}

func (m *Store) UpdateSnapshotRequest(ctx context.Context, s *snapshot.Snapshot) error {
	return m.UpdateSnapshotRequestFn(ctx, s)
}

func (m *Store) GetSnapshotRequests(ctx context.Context, status snapshot.Status) ([]*snapshot.Snapshot, error) {
	return m.GetSnapshotRequestsFn(ctx, status)
}
