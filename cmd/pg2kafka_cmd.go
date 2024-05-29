// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/xataio/pgstream/pkg/stream"

	"github.com/spf13/cobra"
)

var pg2kafkaCmd = &cobra.Command{
	Use:   "pg2kafka",
	Short: "Starts a postgres WAL listener and a kafka batch writer that will push processed messages to a kafka topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		config := parseStreamConfig()
		stream, err := stream.New(ctx, config)
		if err != nil {
			return err
		}

		return stream.PostgresToKafka(ctx)
	},
}
