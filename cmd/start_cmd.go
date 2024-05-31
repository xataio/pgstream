// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/xataio/pgstream/pkg/stream"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the configured pgstream modules",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stream.Start(context.Background(), parseStreamConfig())
	},
}
