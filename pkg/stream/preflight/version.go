// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
)

// pgVersion is a Postgres server version derived from server_version_num
// (e.g. 180004 for PostgreSQL 18.4). For every version pgstream supports (10+)
// the major version is num/10000 and the minor/patch is num%10000.
type pgVersion struct {
	num int
}

func (v pgVersion) major() int { return v.num / 10000 }

// String renders the full major.minor version, e.g. 180004 -> "18.4". Zero
// (unread) renders as an empty string.
func (v pgVersion) String() string {
	if v.num == 0 {
		return ""
	}
	return fmt.Sprintf("%d.%d", v.num/10000, v.num%10000)
}

// PostgresVersionCheck reports the source's PostgreSQL version and, when a
// target is configured, verifies the target runs a major version at least as
// new as the source. pgstream snapshots the source with pg_dump and restores it
// into the target; restoring a dump taken from a newer server into an older one
// is unsupported and can fail on syntax or catalog differences the older server
// doesn't understand. The gate is on the major version — a minor/patch
// difference within the same major does not block a restore.
//
// Target is optional: when nil (e.g. source-only runs) the check is purely
// informational and surfaces just the source version via Details
// (source_version); when set it additionally surfaces target_version and
// reports a finding on an incompatible downgrade. Either way it is the single
// source of truth for version information in the report.
type PostgresVersionCheck struct {
	Source postgres.AcquireFunc
	Target postgres.AcquireFunc

	// sourceVersion and targetVersion are captured during Run and surfaced
	// through Details. Zero-valued until Run has read them; targetVersion stays
	// zero when Target is nil.
	sourceVersion pgVersion
	targetVersion pgVersion
}

func (c *PostgresVersionCheck) Name() string { return "postgres_version" }

func (c *PostgresVersionCheck) Run(ctx context.Context) ([]Finding, error) {
	source, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}
	c.sourceVersion, err = queryPostgresVersion(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("querying source version: %w", err)
	}

	// No target to compare against: report the source version only.
	if c.Target == nil {
		return nil, nil
	}

	target, err := c.Target(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to target: %w", err)
	}
	c.targetVersion, err = queryPostgresVersion(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("querying target version: %w", err)
	}

	if c.targetVersion.major() < c.sourceVersion.major() {
		return []Finding{{
			Message: fmt.Sprintf(
				"source is PostgreSQL %s, target is PostgreSQL %s; restoring a dump from a newer server into an older one is unsupported and may fail. Use a target running PostgreSQL %d or newer.",
				c.sourceVersion, c.targetVersion, c.sourceVersion.major(),
			),
		}}, nil
	}
	return nil, nil
}

// Details exposes the source version, and the target version too when a target
// was compared, so the report records what was inspected regardless of outcome.
func (c *PostgresVersionCheck) Details() map[string]any {
	details := map[string]any{"source_version": c.sourceVersion.String()}
	if c.Target != nil {
		details["target_version"] = c.targetVersion.String()
	}
	return details
}

// queryPostgresVersion reads server_version_num from conn.
func queryPostgresVersion(ctx context.Context, conn postgres.Querier) (pgVersion, error) {
	var num int
	if err := conn.QueryRow(ctx, []any{&num}, "SELECT current_setting('server_version_num')::int"); err != nil {
		return pgVersion{}, err
	}
	return pgVersion{num: num}, nil
}
