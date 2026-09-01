// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xataio/pgstream/internal/postgres"
)

// WALLevelCheck verifies the source Postgres has `wal_level=logical`, which
// pgstream's replication path requires.
type WALLevelCheck struct {
	Source postgres.AcquireFunc
}

func (c *WALLevelCheck) Name() string { return "wal_level" }

func (c *WALLevelCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

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
//
// wal2json is a logical-decoding output plugin, not a SQL extension, so it
// never appears in pg_available_extensions and there is no catalog that lists
// installed output plugins. The only way to detect it with pgstream's
// privileges (a non-superuser REPLICATION role) is to probe actual behaviour:
// create a temporary logical replication slot with the plugin and inspect the
// outcome. The temporary slot is released automatically at session end, and is
// dropped explicitly on success so it never counts against slot headroom.
type WAL2JSONCheck struct {
	Source postgres.AcquireFunc
}

func (c *WAL2JSONCheck) Name() string { return "wal2json" }

// wal2jsonProbeTimeout bounds the slot-creation probe. Creating a logical slot
// builds a consistent snapshot, which waits for in-flight write transactions on
// the source to finish; on a busy primary with a long-running transaction that
// wait is unbounded. Cap it so `pgstream check` reports an actionable error
// instead of hanging the whole preflight run.
const wal2jsonProbeTimeout = 30 * time.Second

func (c *WAL2JSONCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

	slotName, err := wal2jsonProbeSlotName()
	if err != nil {
		return nil, fmt.Errorf("generating probe slot name: %w", err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, wal2jsonProbeTimeout)
	defer cancel()

	var ok bool
	probeErr := conn.QueryRow(probeCtx, []any{&ok},
		"SELECT lsn IS NOT NULL FROM pg_create_logical_replication_slot($1, 'wal2json', true)",
		slotName)
	if probeErr == nil {
		// Plugin is installed and loadable. Drop the temporary slot explicitly
		// so it doesn't count against replication_slot_headroom; the temporary
		// flag auto-drops it at session end regardless. Best-effort: the slot is
		// temporary, so a failed drop is released when the session closes.
		_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
		return nil, nil
	}

	switch {
	case isWAL2JSONMissing(probeErr), isWAL2JSONNotAllowed(probeErr):
		return []Finding{{
			Message: "wal2json output plugin not available on source; install the wal2json package, and on postgres 17.11+ add wal2json to output_plugin_libraries (it defaults to \"pgoutput, test_decoding\") and reload the server — that allowlist is checked before the library is loaded, so an installed wal2json is still refused while it is missing from it",
		}}, nil
	case isProbePreconditionUnmet(probeErr):
		// The probe needs wal_level=logical, the REPLICATION role attribute and a
		// free slot to run at all, and must run against a primary. Those are owned
		// by the dedicated wal_level / replication_role_attr / slot-headroom checks
		// (or aren't a plugin problem), so returning inconclusive here avoids a
		// false "wal2json missing" finding.
		return nil, nil
	default:
		return nil, fmt.Errorf("probing wal2json plugin: %w", probeErr)
	}
}

// wal2jsonProbeSlotName returns a valid, collision-resistant temporary slot
// name for the wal2json probe. The random suffix guards against concurrent
// `pgstream check` runs racing on the same slot name.
func wal2jsonProbeSlotName() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "pgstream_wal2json_probe_" + hex.EncodeToString(b[:]), nil
}

// isWAL2JSONMissing reports whether the probe error indicates the wal2json
// shared library could not be found/loaded — the one error that confidently
// means the plugin is not installed. Postgres raises SQLSTATE 58P01
// (undefined_file) for a missing output-plugin library; the message check is a
// locale-fragile fallback for the rare case the code is not surfaced.
func isWAL2JSONMissing(err error) bool {
	if pgErrCodeIs(err, "58P01") {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "could not access file") ||
		(strings.Contains(msg, "could not load library") && strings.Contains(msg, "wal2json"))
}

// isWAL2JSONNotAllowed reports whether the probe error is postgres refusing
// wal2json because it is absent from the output_plugin_libraries allowlist.
func isWAL2JSONNotAllowed(err error) bool {
	return strings.Contains(err.Error(), "may not be used as an output plugin")
}

// isProbePreconditionUnmet reports whether the probe failed for a reason owned
// by a sibling check or otherwise not indicative of a missing plugin, so the
// wal2json check should stay silent (inconclusive). Classification keys on
// stable SQLSTATE codes (via internal/postgres.MapError's typed errors and the
// raw pgconn code) rather than server-localised message text.
func isProbePreconditionUnmet(err error) bool {
	// 55000 object_not_in_prerequisite_state: wal_level<logical (owned by the
	// wal_level check) or the source is in recovery (a standby).
	var precondition *postgres.ErrPreconditionFailed
	if errors.As(err, &precondition) {
		return true
	}
	// 42501 insufficient_privilege: role lacks REPLICATION (owned by the
	// replication_role_attr check).
	var permission *postgres.ErrPermissionDenied
	if errors.As(err, &permission) {
		return true
	}
	// 53400 configuration_limit_exceeded: no free replication slots (owned by
	// the replication_slot_headroom check).
	return pgErrCodeIs(err, "53400")
}

// pgErrCodeIs reports whether err carries a raw Postgres error with the given
// SQLSTATE code. Only errors MapError leaves untyped (e.g. 58P01, 53400) still
// expose the pgconn.PgError; the mapped classes are matched via their typed
// errors instead.
func pgErrCodeIs(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// ReplicationSlotHeadroomCheck reports whether the source has at least one
// slot still available before max_replication_slots is reached.
type ReplicationSlotHeadroomCheck struct {
	Source postgres.AcquireFunc
}

func (c *ReplicationSlotHeadroomCheck) Name() string { return "replication_slot_headroom" }

func (c *ReplicationSlotHeadroomCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

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
	Source postgres.AcquireFunc
}

func (c *ReplicationRoleAttrCheck) Name() string { return "replication_role_attr" }

func (c *ReplicationRoleAttrCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := c.Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to source: %w", err)
	}

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
