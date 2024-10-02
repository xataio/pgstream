// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Store interface {
	CreateSnapshotRequest(context.Context, *snapshot.Snapshot) error
	UpdateSnapshotRequest(context.Context, *snapshot.Snapshot) error
	GetSnapshotRequests(ctx context.Context, status snapshot.Status) ([]*snapshot.Snapshot, error)
}

const (
	SchemaName = "pgstream"
	TableName  = "snapshot_requests"
)
