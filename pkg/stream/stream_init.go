// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmigrations "github.com/xataio/pgstream/migrations/postgres"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	pgstreamSchema = "pgstream"
)

// Init initialises the pgstream state in the postgres database provided, along
// with creating the relevant replication slot.
func Init(ctx context.Context, pgURL, replicationSlotName string) error {
	conn, err := newPGConn(ctx, pgURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// first create the pgstream schema so that the migrations table is
	// created under it
	if err := createPGStreamSchema(ctx, conn); err != nil {
		return fmt.Errorf("failed to create pgstream schema: %w", err)
	}

	migrator, err := newPGMigrator(pgURL)
	if err != nil {
		return fmt.Errorf("error creating postgres migrator: %w", err)
	}

	if err := migrator.Up(); err != nil {
		return fmt.Errorf("failed to run internal pgstream migrations: %w", err)
	}

	if replicationSlotName == "" {
		replicationSlotName, err = getReplicationSlotName(pgURL)
		if err != nil {
			return err
		}
	}

	if err := createReplicationSlot(ctx, conn, replicationSlotName); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

// TearDown removes the pgstream state from the postgres database provided,
// as well as removing the replication slot.
func TearDown(ctx context.Context, pgURL, replicationSlotName string) error {
	conn, err := newPGConn(ctx, pgURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if replicationSlotName == "" {
		replicationSlotName, err = getReplicationSlotName(pgURL)
		if err != nil {
			return err
		}
	}

	if err := dropReplicationSlot(ctx, conn, replicationSlotName); err != nil {
		return err
	}

	migrator, err := newPGMigrator(pgURL)
	if err != nil {
		return fmt.Errorf("error creating postgres migrator: %w", err)
	}

	if err := migrator.Down(); err != nil {
		return fmt.Errorf("failed to run internal pgstream migrations: %w", err)
	}

	// delete the pgstream schema once the migration tear down has completed
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

func newPGConn(ctx context.Context, pgURL string) (*pgx.Conn, error) {
	pgCfg, err := pgx.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", err)
	}
	pgConn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %w", err)
	}
	return pgConn, nil
}

func newPGMigrator(pgURL string) (*migrate.Migrate, error) {
	src := bindata.Resource(pgmigrations.AssetNames(), pgmigrations.Asset)
	d, err := bindata.WithInstance(src)
	if err != nil {
		return nil, err
	}

	url := pgURL + "&search_path=pgstream"
	return migrate.NewWithSourceInstance("go-bindata", d, url)
}

func isDuplicateObject(err error) bool {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		return false
	}

	return pgerr.Code == pgerrcode.DuplicateObject
}

func getReplicationSlotName(pgURL string) (string, error) {
	cfg, err := pgx.ParseConfig(pgURL)
	if err != nil {
		return "", err
	}
	dbName := "postgres"
	if cfg.Database != "" {
		dbName = cfg.Database
	}
	return pglib.DefaultReplicationSlotName(dbName), nil
}
