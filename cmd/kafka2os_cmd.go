// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/xataio/pgstream/pkg/stream"

	"github.com/spf13/cobra"
)

var kafka2osCmd = &cobra.Command{
	Use:   "kafka2os",
	Short: "Starts a kafka reader and an opensearch indexer to process the messages from the kafka topic into opensearch",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		config := parseStreamConfig()
		stream, err := stream.New(ctx, config)
		if err != nil {
			return err
		}

		return stream.KafkaToOpensearch(ctx)
	},
}
