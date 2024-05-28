// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	pgmigrations "github.com/xataio/pgstream/migrations/postgres"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Stream struct {
	pgURL string
	pgCfg *pgx.ConnConfig
}

const (
	pgstreamSchema = "pgstream"
)

func New(ctx context.Context, pgURL string) (*Stream, error) {
	pgCfg, err := pgx.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", err)
	}

	return &Stream{
		pgURL: pgURL,
		pgCfg: pgCfg,
	}, nil
}

func (s *Stream) Init(ctx context.Context) error {
	// first create the pgstream schema so that the migrations table is
	// created under it
	if err := s.createPGStreamSchema(ctx); err != nil {
		return fmt.Errorf("failed to create pgstream schema: %w", err)
	}

	migrator, err := newPGMigrator(s.pgURL)
	if err != nil {
		return fmt.Errorf("error creating postgres migrator: %w", err)
	}

	if err := migrator.Up(); err != nil {
		return fmt.Errorf("failed to run internal pgstream migrations: %w", err)
	}

	if err := s.createReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

func (s *Stream) TearDown(ctx context.Context) error {
	if err := s.dropReplicationSlot(ctx); err != nil {
		return err
	}

	migrator, err := newPGMigrator(s.pgURL)
	if err != nil {
		return fmt.Errorf("error creating postgres migrator: %w", err)
	}

	if err := migrator.Down(); err != nil {
		return fmt.Errorf("failed to run internal pgstream migrations: %w", err)
	}

	// delete the pgstream schema once the migration tear down has completed
	if err := s.dropPGStreamSchema(ctx); err != nil {
		return fmt.Errorf("failed to drop pgstream schema: %w", err)
	}

	return nil
}

func (s *Stream) createPGStreamSchema(ctx context.Context) error {
	conn, err := s.newPGConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pgstreamSchema)); err != nil {
		return fmt.Errorf("failed to create postgres pgstream schema: %w", err)
	}

	return nil
}

func (s *Stream) dropPGStreamSchema(ctx context.Context) error {
	conn, err := s.newPGConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgstreamSchema)); err != nil {
		return fmt.Errorf("failed to drop postgres pgstream schema: %w", err)
	}

	return nil
}

func (s *Stream) createReplicationSlot(ctx context.Context) error {
	pgConn, err := s.newPGConn(ctx)
	if err != nil {
		return err
	}
	defer pgConn.Close(ctx)

	dbName := s.pgCfg.Database
	if s.pgCfg.Database == "" {
		dbName = "postgres"
	}

	_, err = pgConn.Exec(ctx, fmt.Sprintf(`SELECT 'init' FROM pg_create_logical_replication_slot ('pgstream_%s_slot', 'wal2json')`, dbName))
	if err != nil && !isDuplicateObject(err) {
		return err
	}
	return nil
}

func (s *Stream) dropReplicationSlot(ctx context.Context) error {
	pgConn, err := pgx.ConnectConfig(ctx, s.pgCfg)
	if err != nil {
		return fmt.Errorf("failed to create postgres client: %w", err)
	}
	defer pgConn.Close(ctx)

	dbName := s.pgCfg.Database
	if s.pgCfg.Database == "" {
		dbName = "postgres"
	}
	_, err = pgConn.Exec(ctx, fmt.Sprintf(`SELECT pg_drop_replication_slot('pgstream_%s_slot') from pg_replication_slots where slot_name = 'pgstream_%s_slot'`, dbName, dbName))
	return err
}

func (s *Stream) newPGConn(ctx context.Context) (*pgx.Conn, error) {
	pgConn, err := pgx.ConnectConfig(ctx, s.pgCfg)
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
