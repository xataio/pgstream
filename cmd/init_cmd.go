// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"

	pgmigrations "github.com/xataio/pgstream/migrations/postgres"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
	"github.com/jackc/pgx/v5"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialises pgstream, creating the relevant tables/functions/triggers under the configured internal schema",
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("initialising pgstream...").Start()

		// first create the pgstream schema so that the migrations table is
		// created under it
		if err := createPGStreamSchema(); err != nil {
			sp.Fail(fmt.Sprintf("failed to create pgstream schema: %v", err))
			return err
		}

		m, err := newPGMigrator()
		if err != nil {
			sp.Fail(fmt.Sprintf("failed setting up pgstream migrator: %v", err))
			return err
		}

		if err := m.Up(); err != nil {
			sp.Fail(fmt.Sprintf("failed to run internal pgstream migrations: %v", err))
			return err
		}

		sp.Success("pgstream initialisation complete")
		return nil
	},
}

var tearDownCmd = &cobra.Command{
	Use:   "tear-down",
	Short: "It tears down any pgstream setup, removing all the relevant tables/functions/triggers and the internal pgstream schema.",
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("tearing down pgstream...").Start()

		// wrap assets into Resource
		m, err := newPGMigrator()
		if err != nil {
			sp.Fail(fmt.Sprintf("failed setting up pgstream migrator: %v", err))
			return err
		}

		if err := m.Down(); err != nil {
			sp.Fail(fmt.Sprintf("failed to run internal pgstream migrations: %v", err))
			return err
		}

		// delete the pgstream schema once the migration tear down has completed
		if err := dropPGStreamSchema(); err != nil {
			sp.Fail(fmt.Sprintf("failed to drop pgstream schema: %v", err))
			return err
		}

		sp.Success("pgstream tear down complete")
		return nil
	},
}

func newPGMigrator() (*migrate.Migrate, error) {
	src := bindata.Resource(pgmigrations.AssetNames(), pgmigrations.Asset)
	d, err := bindata.WithInstance(src)
	if err != nil {
		return nil, err
	}

	pgURL := viper.GetString("PG_URL") + "&search_path=pgstream"
	return migrate.NewWithSourceInstance("go-bindata", d, pgURL)
}

func createPGStreamSchema() error {
	pgConn, err := newPGConn()
	if err != nil {
		return err
	}

	if _, err := pgConn.Exec(context.Background(), "CREATE SCHEMA IF NOT EXISTS pgstream"); err != nil {
		return fmt.Errorf("failed to create postgres pgstream schema: %w", err)
	}

	return nil
}

func dropPGStreamSchema() error {
	pgConn, err := newPGConn()
	if err != nil {
		return err
	}

	if _, err := pgConn.Exec(context.Background(), "DROP SCHEMA IF EXISTS pgstream CASCADE"); err != nil {
		return fmt.Errorf("failed to drop postgres pgstream schema: %w", err)
	}

	return nil
}

func newPGConn() (*pgx.Conn, error) {
	pgURL := viper.GetString("PG_URL")
	pgCfg, err := pgx.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("failed parsing postgres connection string: %w", err)
	}
	ctx := context.Background()
	pgConn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres client: %w", err)
	}
	return pgConn, nil
}
