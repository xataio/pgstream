// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	migratorlib "github.com/xataio/pgstream/internal/migrator"
	"github.com/xataio/pgstream/internal/phase"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type InitConfig struct {
	PostgresURL               string
	ReplicationSlotName       string
	InjectorMigrationsEnabled bool
	MigrationsOnly            bool
	SlotOnly                  bool
	Upgrade                   bool
	// PhaseTracker is optional runtime state for stream.Run; Init/Destroy ignore it.
	PhaseTracker *phase.Tracker
	// Logger is optional. Init uses it to report progress that would otherwise
	// be invisible, such as a replication slot creation that is blocked rather
	// than slow. Defaults to a no-op logger.
	Logger loglib.Logger
}

type InitOption func(*InitConfig)

// WithInitLogger sets the logger Init and Destroy report progress through.
func WithInitLogger(logger loglib.Logger) InitOption {
	return func(cfg *InitConfig) {
		cfg.Logger = logger
	}
}

func WithMigrationsOnly() InitOption {
	return func(cfg *InitConfig) {
		cfg.MigrationsOnly = true
	}
}

func WithSlotOnly() InitOption {
	return func(cfg *InitConfig) {
		cfg.SlotOnly = true
	}
}

func WithUpgrade() InitOption {
	return func(cfg *InitConfig) {
		cfg.Upgrade = true
	}
}

// WithPhaseTracker registers a tracker updated as the pipeline moves between
// snapshot and replication phases. Used by stream.Run; ignored by Init/Destroy.
func WithPhaseTracker(t *phase.Tracker) InitOption {
	return func(cfg *InitConfig) {
		cfg.PhaseTracker = t
	}
}

const (
	pgstreamSchema = "pgstream"
)

var (
	errMissingPostgresURL    = errors.New("postgres URL is required")
	errMigrationsAndSlotOnly = errors.New("migrations-only and slot-only are mutually exclusive")
	// upgrade only cleans up v0.9.x schema state, which slot-only skips
	// entirely, so accepting both would silently drop the upgrade
	errUpgradeAndSlotOnly = errors.New("upgrade and slot-only are mutually exclusive")
)

// validateRestrictions rejects flag combinations that select disjoint halves of
// the work, so a caller never gets one silently ignored.
func (c *InitConfig) validateRestrictions() error {
	if c.MigrationsOnly && c.SlotOnly {
		return errMigrationsAndSlotOnly
	}
	if c.Upgrade && c.SlotOnly {
		return errUpgradeAndSlotOnly
	}
	return nil
}

// Init initialises the pgstream state in the postgres database provided, along
// with creating the relevant replication slot if it doesn't already exist.
func Init(ctx context.Context, config *InitConfig) error {
	if config.PostgresURL == "" {
		return errMissingPostgresURL
	}
	if err := config.validateRestrictions(); err != nil {
		return err
	}

	conn, err := newPGConn(ctx, config.PostgresURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// the schema and migrations are skipped entirely in slot-only mode: the
	// target may be a read only standby, where every statement below would fail
	if !config.SlotOnly {
		// first create the pgstream schema so that the migrations table is
		// created under it
		if err := createPGStreamSchema(ctx, conn); err != nil {
			return fmt.Errorf("failed to create pgstream schema: %w", err)
		}

		if config.Upgrade {
			if err := cleanupV09xState(ctx, conn); err != nil {
				return fmt.Errorf("failed to clean up v0.9.x state: %w", err)
			}
		}

		migrationAssets := []*migratorlib.MigrationAssets{
			migratorlib.GetCoreMigrationAssets(),
		}
		if config.InjectorMigrationsEnabled {
			migrationAssets = append(migrationAssets, migratorlib.GetInjectorMigrationAssets())
		}
		migrator, err := migratorlib.NewPGMigrator(config.PostgresURL, migrationAssets)
		if err != nil {
			return fmt.Errorf("error creating postgres migrator: %w", err)
		}

		if err := migrator.Up(); err != nil && !errors.Is(err, migratorlib.ErrNoChange) {
			return fmt.Errorf("failed to run internal pgstream migrations: %w", err)
		}

		// if only migrations need to be run, return early
		if config.MigrationsOnly {
			return nil
		}
	}

	if config.ReplicationSlotName == "" {
		config.ReplicationSlotName, err = getReplicationSlotName(config.PostgresURL)
		if err != nil {
			return err
		}
	}

	if err := pglib.IsValidReplicationSlotName(config.ReplicationSlotName); err != nil {
		return err
	}

	// check if the replication slot already exists
	exists, err := replicationSlotExists(ctx, conn, config.ReplicationSlotName)
	if err != nil {
		return fmt.Errorf("failed to check if replication slot exists: %w", err)
	}
	if exists {
		// return early if the replication slot already exists
		return nil
	}

	stopHint := warnOnSlowSlotCreation(ctx, config)
	err = createReplicationSlot(ctx, conn, config.ReplicationSlotName)
	stopHint()
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

// slotCreationHintDelay is how long slot creation is allowed to run before the
// hint is logged. Long enough that a normally slow creation stays quiet, short
// enough to reach someone still watching the command.
const slotCreationHintDelay = 10 * time.Second

// warnOnSlowSlotCreation logs a hint if slot creation has not finished within
// slotCreationHintDelay, and returns a function that cancels it.
//
// Creating a logical slot on a standby blocks until the primary emits an
// xl_running_xacts record, which is what lets the standby build a consistent
// catalog snapshot. A primary taking writes emits one within seconds, but one
// that has gone quiet may never emit one at all — so the call can wait forever
// with no error and no timeout. Without this hint that is indistinguishable
// from a slow connection, and the fix (a statement on a different server) is
// not something a caller would guess.
func warnOnSlowSlotCreation(ctx context.Context, config *InitConfig) (stop func()) {
	logger := config.Logger
	if logger == nil {
		return func() {}
	}

	hintCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-hintCtx.Done():
		case <-time.After(slotCreationHintDelay):
			logger.Warn(nil, "still waiting to create the replication slot", loglib.Fields{
				"slot_name": config.ReplicationSlotName,
				"hint": "if the source is a read replica, this waits for a running-xacts record from its primary. " +
					"Run 'SELECT pg_log_standby_snapshot()' on the primary to unblock it",
			})
		}
	}()

	return cancel
}

// Destroy removes the pgstream state from the postgres database provided,
// as well as removing the replication slot.
func Destroy(ctx context.Context, config *InitConfig) error {
	if config.PostgresURL == "" {
		return errMissingPostgresURL
	}
	if err := config.validateRestrictions(); err != nil {
		return err
	}

	conn, err := newPGConn(ctx, config.PostgresURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// in slot-only mode the pgstream schema is left in place, so the emit_ddl
	// event trigger keeps working for anything still replicating from it
	if config.SlotOnly {
		return dropConfiguredReplicationSlot(ctx, conn, config)
	}

	migrationAssets := []*migratorlib.MigrationAssets{
		migratorlib.GetCoreMigrationAssets(),
	}
	if config.InjectorMigrationsEnabled {
		migrationAssets = append(migrationAssets, migratorlib.GetInjectorMigrationAssets())
	}
	migrator, err := migratorlib.NewPGMigrator(config.PostgresURL, migrationAssets)
	if err != nil {
		return fmt.Errorf("error creating postgres migrator: %w", err)
	}

	if err := migrator.Down(); err != nil && !errors.Is(err, migratorlib.ErrNoChange) {
		return fmt.Errorf("failed to revert internal pgstream migrations: %w", err)
	}

	// if only migrations need to be reverted, delete the schema migration
	// tables and return early. Otherwise the pgstream schema drop will take
	// care of cleaning up the migration tables.
	if config.MigrationsOnly {
		if err := dropMigrationTables(ctx, conn, migrationAssets); err != nil {
			return err
		}
		return nil
	}

	// delete the pgstream schema once the migration destroy has completed
	if err := dropPGStreamSchema(ctx, conn); err != nil {
		return fmt.Errorf("failed to drop pgstream schema: %w", err)
	}

	return dropConfiguredReplicationSlot(ctx, conn, config)
}

// dropConfiguredReplicationSlot resolves the slot name the same way Init does,
// defaulting it from the database name when it isn't configured, and drops it.
func dropConfiguredReplicationSlot(ctx context.Context, conn *pgx.Conn, config *InitConfig) error {
	if config.ReplicationSlotName == "" {
		var err error
		config.ReplicationSlotName, err = getReplicationSlotName(config.PostgresURL)
		if err != nil {
			return err
		}
	}

	if err := pglib.IsValidReplicationSlotName(config.ReplicationSlotName); err != nil {
		return err
	}

	return dropReplicationSlot(ctx, conn, config.ReplicationSlotName)
}

func createPGStreamSchema(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pgstreamSchema)); err != nil {
		return fmt.Errorf("failed to create postgres pgstream schema: %w", err)
	}

	return nil
}

func dropPGStreamSchema(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgstreamSchema)); err != nil {
		return fmt.Errorf("failed to drop postgres pgstream schema: %w", err)
	}

	return nil
}

func dropMigrationTables(ctx context.Context, conn *pgx.Conn, migrationAssets []*migratorlib.MigrationAssets) error {
	for _, assets := range migrationAssets {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pglib.QuoteQualifiedIdentifier("pgstream", assets.TableName))); err != nil {
			return fmt.Errorf("failed to delete migration table %s: %w", assets.TableName, err)
		}
	}
	return nil
}

func createReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	_, err := conn.Exec(ctx, `SELECT 'init' FROM pg_create_logical_replication_slot($1, 'wal2json')`, slotName)
	if err != nil && !isDuplicateObject(err) {
		return err
	}
	return nil
}

func dropReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	_, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = $1`, slotName)
	return err
}

func replicationSlotExists(ctx context.Context, conn *pgx.Conn, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slotName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func newPGConn(ctx context.Context, pgURL string) (*pgx.Conn, error) {
	pgCfg, err := pglib.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", err)
	}
	pgConn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %w", err)
	}
	return pgConn, nil
}

func isDuplicateObject(err error) bool {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		return false
	}

	return pgerr.Code == pgerrcode.DuplicateObject
}

func getReplicationSlotName(pgURL string) (string, error) {
	cfg, err := pglib.ParseConfig(pgURL)
	if err != nil {
		return "", err
	}
	dbName := "postgres"
	if cfg.Database != "" {
		dbName = cfg.Database
	}
	return pglib.DefaultReplicationSlotName(dbName), nil
}

// cleanupV09xState removes database objects that were created by v0.9.x but
// are no longer needed in v1.0. The cleanup is idempotent — all statements use
// IF EXISTS so they are safe to run concurrently or repeatedly.
func cleanupV09xState(ctx context.Context, conn *pgx.Conn) error {
	// Check if v0.9.x state exists by looking for the old schema_migrations
	// table (v1.0 uses schema_migrations_core/schema_migrations_injector instead).
	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'pgstream' AND table_name = 'schema_migrations'
		)`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking for v0.9.x state: %w", err)
	}
	if !exists {
		return nil
	}

	// Drop all pgstream event triggers first (they depend on pgstream.log_schema)
	rows, err := conn.Query(ctx, `SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'pgstream_%'`)
	if err != nil {
		return fmt.Errorf("querying event triggers: %w", err)
	}
	var triggers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("scanning event trigger name: %w", err)
		}
		triggers = append(triggers, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating event triggers: %w", err)
	}
	for _, name := range triggers {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP EVENT TRIGGER IF EXISTS %s", pglib.QuoteIdentifier(name))); err != nil {
			return fmt.Errorf("dropping event trigger %s: %w", name, err)
		}
	}

	// v0.9.x objects that are not present in v1.0
	cleanupStatements := []string{
		"DROP FUNCTION IF EXISTS pgstream.log_schema()",
		"DROP FUNCTION IF EXISTS pgstream.get_schema(text)",
		"DROP FUNCTION IF EXISTS pgstream.refresh_schema()",
		"DROP TABLE IF EXISTS pgstream.schema_log",
		"DROP TABLE IF EXISTS pgstream.schema_migrations",
	}

	for _, stmt := range cleanupStatements {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("executing %q: %w", stmt, err)
		}
	}

	return nil
}
