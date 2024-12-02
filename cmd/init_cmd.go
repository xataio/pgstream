// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/xataio/pgstream/pkg/stream"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialises pgstream, creating the replication slot and the relevant tables/functions/triggers under the configured internal schema.",
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("initialising pgstream...").Start()

		if err := stream.Init(context.Background(), pgURL(), replicationSlotName()); err != nil {
			sp.Fail(err.Error())
			return err
		}

		sp.Success("pgstream initialisation complete")
		return nil
	},
}

var tearDownCmd = &cobra.Command{
	Use:   "tear-down",
	Short: "It tears down any pgstream setup, removing the replication slot and all the relevant tables/functions/triggers, the internal pgstream schema.",
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("tearing down pgstream...").Start()

		if err := stream.TearDown(context.Background(), pgURL(), replicationSlotName()); err != nil {
			sp.Fail(err.Error())
			return err
		}

		sp.Success("pgstream tear down complete")
		return nil
	},
}
