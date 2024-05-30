// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/replication"
	pgreplication "github.com/xataio/pgstream/internal/replication/postgres"
	"github.com/xataio/pgstream/pkg/wal"
)

type Checkpointer struct {
	syncer lsnSyncer
}

type Config struct {
	Replication pgreplication.Config
}

type lsnSyncer interface {
	SyncLSN(ctx context.Context, lsn replication.LSN) error
}

func New(ctx context.Context, cfg Config) (*Checkpointer, error) {
	replicationHandler, err := pgreplication.NewHandler(ctx, cfg.Replication)
	if err != nil {
		return nil, fmt.Errorf("postgres checkpointer: create replication handler: %w", err)
	}

	return &Checkpointer{
		syncer: replicationHandler,
	}, nil
}

func (c *Checkpointer) SyncLSN(ctx context.Context, positions []wal.CommitPosition) error {
	// we only need the max pg wal offset
	if len(positions) == 0 {
		return nil
	}

	max := positions[0].PGPos
	for _, position := range positions {
		if position.PGPos > max {
			max = position.PGPos
		}
	}

	return c.syncer.SyncLSN(ctx, replication.LSN(max))
}
