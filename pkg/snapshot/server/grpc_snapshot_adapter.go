// SPDX-License-Identifier: Apache-2.0

package server

import (
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/api"
)

type apiAdapter interface {
	toSnapshot(*api.SnapshotRequest) *snapshot.Snapshot
}

type adapter struct{}

func (a *adapter) toSnapshot(req *api.SnapshotRequest) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		SchemaName:          req.SchemaName,
		TableName:           req.TableName,
		IdentityColumnNames: req.IdentityColumnNames,
	}
}
