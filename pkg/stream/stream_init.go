// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	migratorlib "github.com/xataio/pgstream/internal/migrator"
	pglib "github.com/xataio/pgstream/internal/postgres"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type InitConfig struct {
	PostgresURL               string
	ReplicationSlotName       string
	InjectorMigrationsEnabled bool
}

const (
	pgstreamSchema = "pgstream"
)

var errMissingPostgresURL = errors.New("postgres URL is required")

// Init initialises the pgstream state in the postgres database provided, along
// with creating the relevant replication slot if it doesn't already exist.
func Init(ctx context.Context, config *InitConfig) error {
	if config.PostgresURL == "" {
		return errMissingPostgresURL
	}

	conn, err := newPGConn(ctx, config.PostgresURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// first create the pgstream schema so that the migrations table is
	// created under it
	if err := createPGStreamSchema(ctx, conn); err != nil {
		return fmt.Errorf("failed to create pgstream schema: %w", err)
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

	if err := createReplicationSlot(ctx, conn, config.ReplicationSlotName); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

// Destroy removes the pgstream state from the postgres database provided,
// as well as removing the replication slot.
func Destroy(ctx context.Context, config *InitConfig) error {
	if config.PostgresURL == "" {
		return errMissingPostgresURL
	}

	conn, err := newPGConn(ctx, config.PostgresURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if config.ReplicationSlotName == "" {
		config.ReplicationSlotName, err = getReplicationSlotName(config.PostgresURL)
		if err != nil {
			return err
		}
	}

	if err := pglib.IsValidReplicationSlotName(config.ReplicationSlotName); err != nil {
		return err
	}

	if err := dropReplicationSlot(ctx, conn, config.ReplicationSlotName); err != nil {
		return err
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

	// delete the pgstream schema once the migration destroy has completed
	if err := dropPGStreamSchema(ctx, conn); err != nil {
		return fmt.Errorf("failed to drop pgstream schema: %w", err)
	}

	return nil
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

func createReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(`SELECT 'init' FROM pg_create_logical_replication_slot ('%s', 'wal2json')`, slotName))
	if err != nil && !isDuplicateObject(err) {
		return err
	}
	return nil
}

func dropReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(`SELECT pg_drop_replication_slot('%[1]s') from pg_replication_slots where slot_name = '%[1]s'`, slotName))
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
