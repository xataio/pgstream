// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"
)

// SourceSnapshotInstanceCheck verifies the source URL resolves to a single
// Postgres instance, which parallel data snapshotting requires. The data
// snapshot generator exports a transaction snapshot on one connection and
// imports it (SET TRANSACTION SNAPSHOT) on other connections; an exported
// snapshot is instance-local, so a load-balanced source (an Aurora/RDS reader
// endpoint, or a pooler spanning instances) that routes some connections to a
// different instance fails with `snapshot "…" does not exist`.
//
// The probe is probabilistic (see ProbeExportedSnapshotVisibility): a run that
// happens to route every probe connection to the exporting instance reports no
// finding.
type SourceSnapshotInstanceCheck struct {
	Probe  func(ctx context.Context, probes int) (missing int, err error)
	Probes int
}

func (c *SourceSnapshotInstanceCheck) Name() string {
	return "source_snapshot_single_instance"
}

func (c *SourceSnapshotInstanceCheck) Run(ctx context.Context) ([]Finding, error) {
	missing, err := c.Probe(ctx, c.Probes)
	if missing > 0 {
		return []Finding{{Message: fmt.Sprintf(
			"source appears to be load-balanced across multiple Postgres instances: an exported "+
				"snapshot was not visible on %d of %d probe connections. Parallel data snapshotting "+
				"requires every connection to reach the same instance — point the source at a "+
				"single-instance / writer endpoint (Aurora/RDS reader endpoints and instance-spanning "+
				"poolers are unsupported for snapshots).",
			missing, c.Probes,
		)}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("running snapshot instance probe: %w", err)
	}
	return nil, nil
}
