// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// Checkpointer is a postgres implementation of a wal checkpointer. It syncs the
// LSN to postgres.
type Checkpointer struct {
	syncer lsnSyncer
	parser replication.LSNParser
}

type Config struct {
	Replication pgreplication.Config
}

type lsnSyncer interface {
	SyncLSN(ctx context.Context, lsn replication.LSN) error
	Close() error
}

// New returns a postgres checkpointer that syncs the LSN to postgres on demand.
func New(syncer lsnSyncer) *Checkpointer {
	return &Checkpointer{
		syncer: syncer,
		parser: pgreplication.NewLSNParser(),
	}
}

func (c *Checkpointer) SyncLSN(ctx context.Context, positions []wal.CommitPosition) error {
	if len(positions) == 0 {
		return nil
	}

	// we only need the max pg wal offset
	var max replication.LSN
	for _, position := range positions {
		lsn, err := c.parser.FromString(string(position))
		if err != nil {
			return err
		}
		if lsn > max {
			max = lsn
		}
	}

	return c.syncer.SyncLSN(ctx, replication.LSN(max))
}

func (c *Checkpointer) Close() error {
	return c.syncer.Close()
}
