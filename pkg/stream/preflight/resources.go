// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
)

// SnapshotConnectionsCheck verifies the source Postgres has enough spare
// connection slots to serve the snapshot's peak concurrency
// (snapshot_workers × table_workers) on top of what is already in use, without
// exceeding max_connections. Non-superuser roles cannot use the slots reserved
// by superuser_reserved_connections, so those are excluded from the headroom.
type SnapshotConnectionsCheck struct {
	Source postgres.AcquireFunc
	// Demand is the number of concurrent connections the snapshot will open at
	// peak (snapshot_workers × table_workers).
	Demand uint
}

func (c *SnapshotConnectionsCheck) Name() string { return "snapshot_connection_headroom" }

func (c *SnapshotConnectionsCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	var maxConns, reserved, used int
	err = conn.QueryRow(ctx, []any{&maxConns, &reserved, &used}, `
		SELECT
		  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections'),
		  (SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections'),
		  (SELECT count(*)::int FROM pg_stat_activity)
	`)
	if err != nil {
		return nil, fmt.Errorf("querying connection limits: %w", err)
	}

	available := maxConns - reserved - used
	if available < 0 {
		available = 0
	}
	if int(c.Demand) > available {
		return []Finding{{
			Message: fmt.Sprintf(
				"snapshot needs %d concurrent connections (snapshot_workers × table_workers) but source has only %d available (max_connections=%d, superuser_reserved_connections=%d, %d in use); lower snapshot_workers/table_workers, raise max_connections (requires restart), or reduce existing connections",
				c.Demand, available, maxConns, reserved, used),
		}}, nil
	}
	return nil, nil
}
