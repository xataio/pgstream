// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/pkg/snapshot"
)

type Store interface {
	// CreateSnapshotRequest records a new request. It returns an error
	// wrapping ErrNonCompletedRequestExists when a non-completed request for
	// the same schema and table list is already recorded.
	CreateSnapshotRequest(context.Context, *snapshot.Request) error
	UpdateSnapshotRequest(context.Context, *snapshot.Request) error
	// LockSnapshotRequests takes a lock per request on its schema and table
	// list, held until release is called or the process exits, so only one
	// run can work on a given scope at a time. It returns an error wrapping
	// ErrSnapshotRequestLocked when another run holds one of the locks.
	LockSnapshotRequests(context.Context, []*snapshot.Request) (release func(), err error)
	// ResumeSnapshotRequest takes over the non-completed request recorded for
	// the same schema and table list as the one on input, whatever its mode,
	// and marks it in progress with the mode on input. It reports whether a
	// request was taken over. The caller must hold the request lock.
	ResumeSnapshotRequest(context.Context, *snapshot.Request) (bool, error)
	GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error)
	GetSnapshotRequestsBySchema(ctx context.Context, schema string) ([]*snapshot.Request, error)
	Close() error
}

var (
	ErrNonCompletedRequestExists = errors.New("a non-completed snapshot request already exists")
	ErrSnapshotRequestLocked     = errors.New("snapshot request locked by another run")
)

const (
	SchemaName = "pgstream"
	TableName  = "snapshot_requests"
)
