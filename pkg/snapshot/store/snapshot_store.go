// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Store interface {
	CreateSnapshotRequest(context.Context, *snapshot.Request) error
	UpdateSnapshotRequest(context.Context, *snapshot.Request) error
	GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error)
	GetSnapshotRequestsBySchema(ctx context.Context, schema string) ([]*snapshot.Request, error)
	Close() error
}

const (
	SchemaName = "pgstream"
	TableName  = "snapshot_requests"
)
