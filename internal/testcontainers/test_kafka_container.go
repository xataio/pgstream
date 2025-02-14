// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupKafkaContainer(ctx context.Context, brokers *[]string) (cleanup, error) {
	ctr, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Kafka Server started").
				WithOccurrence(1).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start kafka container: %w", err)
	}

	*brokers, err = ctr.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving brokers for kafka container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}
