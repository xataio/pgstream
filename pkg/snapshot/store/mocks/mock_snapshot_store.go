// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Store struct {
	CreateSnapshotRequestFn       func(context.Context, *snapshot.Request) error
	UpdateSnapshotRequestFn       func(context.Context, uint, *snapshot.Request) error
	GetSnapshotRequestsByStatusFn func(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error)
	GetSnapshotRequestsBySchemaFn func(ctx context.Context, s string) ([]*snapshot.Request, error)
	updateSnapshotRequestCalls    uint
}

func (m *Store) CreateSnapshotRequest(ctx context.Context, s *snapshot.Request) error {
	return m.CreateSnapshotRequestFn(ctx, s)
}

func (m *Store) UpdateSnapshotRequest(ctx context.Context, s *snapshot.Request) error {
	m.updateSnapshotRequestCalls++
	return m.UpdateSnapshotRequestFn(ctx, m.updateSnapshotRequestCalls, s)
}

func (m *Store) GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
	return m.GetSnapshotRequestsByStatusFn(ctx, status)
}

func (m *Store) GetSnapshotRequestsBySchema(ctx context.Context, s string) ([]*snapshot.Request, error) {
	return m.GetSnapshotRequestsBySchemaFn(ctx, s)
}

func (m *Store) Close() error {
	return nil
}
