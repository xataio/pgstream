// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
)

// WALLevelCheck verifies the source Postgres has `wal_level=logical`, which
// pgstream's replication path requires.
type WALLevelCheck struct {
	URL string
}

func (c *WALLevelCheck) Name() string { return "wal_level" }

func (c *WALLevelCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := postgres.NewConn(ctx, c.URL)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}
	defer conn.Close(ctx)

	var level string
	if err := conn.QueryRow(ctx, []any{&level}, "SHOW wal_level"); err != nil {
		return nil, fmt.Errorf("querying wal_level: %w", err)
	}
	if level != "logical" {
		return []Finding{{
			Message: fmt.Sprintf("wal_level=%q on source; set wal_level=logical in postgresql.conf and restart for logical replication", level),
		}}, nil
	}
	return nil, nil
}

// WAL2JSONCheck verifies that the wal2json output plugin is installed and
// loadable on the source. pgstream decodes WAL through wal2json.
type WAL2JSONCheck struct {
	URL string
}

func (c *WAL2JSONCheck) Name() string { return "wal2json" }

func (c *WAL2JSONCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := postgres.NewConn(ctx, c.URL)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}
	defer conn.Close(ctx)

	var present int
	if err := conn.QueryRow(ctx, []any{&present}, "SELECT count(*)::int FROM pg_available_extensions WHERE name = 'wal2json'"); err != nil {
		return nil, fmt.Errorf("querying pg_available_extensions: %w", err)
	}
	if present == 0 {
		return []Finding{{
			Message: "wal2json output plugin not available on source; install the wal2json package and verify it appears in pg_available_extensions",
		}}, nil
	}
	return nil, nil
}

// ReplicationSlotHeadroomCheck reports whether the source has at least one
// slot still available before max_replication_slots is reached.
type ReplicationSlotHeadroomCheck struct {
	URL string
}

func (c *ReplicationSlotHeadroomCheck) Name() string { return "replication_slot_headroom" }

func (c *ReplicationSlotHeadroomCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := postgres.NewConn(ctx, c.URL)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}
	defer conn.Close(ctx)

	var maxSlots, usedSlots int
	err = conn.QueryRow(ctx, []any{&maxSlots, &usedSlots}, `
		SELECT
		  (SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'),
		  (SELECT count(*)::int FROM pg_replication_slots)
	`)
	if err != nil {
		return nil, fmt.Errorf("querying replication slots: %w", err)
	}
	if usedSlots >= maxSlots {
		return []Finding{{
			Message: fmt.Sprintf("no replication slot headroom: %d/%d slots in use; raise max_replication_slots (requires restart) or drop unused slots", usedSlots, maxSlots),
		}}, nil
	}
	return nil, nil
}

// ReplicationRoleAttrCheck verifies the current source role has the
// REPLICATION attribute, which is required to open a logical replication slot.
type ReplicationRoleAttrCheck struct {
	URL string
}

func (c *ReplicationRoleAttrCheck) Name() string { return "replication_role_attr" }

func (c *ReplicationRoleAttrCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := postgres.NewConn(ctx, c.URL)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}
	defer conn.Close(ctx)

	var roleName string
	var hasReplication bool
	if err := conn.QueryRow(ctx, []any{&roleName, &hasReplication}, "SELECT rolname, rolreplication FROM pg_roles WHERE rolname = current_user"); err != nil {
		return nil, fmt.Errorf("querying pg_roles: %w", err)
	}
	if !hasReplication {
		return []Finding{{
			Message: fmt.Sprintf("source role %q lacks the REPLICATION attribute; run ALTER ROLE %s REPLICATION as a superuser", roleName, roleName),
		}}, nil
	}
	return nil, nil
}
