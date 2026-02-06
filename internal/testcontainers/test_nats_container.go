// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const natsImage = "nats:2.10-alpine"

func SetupNATSContainer(ctx context.Context, natsURL *string) (cleanup, error) {
	req := testcontainers.ContainerRequest{
		Image:        natsImage,
		ExposedPorts: []string{"4222/tcp"},
		Cmd:          []string{"-js"}, // enable JetStream
		WaitingFor: wait.ForLog("Server is ready").
			WithOccurrence(1).
			WithStartupTimeout(10 * time.Second),
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start nats container: %w", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving host for nats container: %w", err)
	}

	mappedPort, err := ctr.MappedPort(ctx, "4222/tcp")
	if err != nil {
		return nil, fmt.Errorf("retrieving mapped port for nats container: %w", err)
	}

	*natsURL = fmt.Sprintf("nats://%s:%s", host, mappedPort.Port())

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}
