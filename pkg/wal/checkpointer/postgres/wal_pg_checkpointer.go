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
	parser replication.LSNParser
}

type Config struct {
	Replication pgreplication.Config
}

type lsnSyncer interface {
	SyncLSN(ctx context.Context, lsn replication.LSN) error
	Close() error
}

func NewWithHandler(syncer lsnSyncer) *Checkpointer {
	return &Checkpointer{
		syncer: syncer,
		parser: pgreplication.NewLSNParser(),
	}
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
