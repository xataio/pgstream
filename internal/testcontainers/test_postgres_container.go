// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type cleanup func() error

type PostgresImage string

const (
	Postgres14 PostgresImage = "debezium/postgres:14-alpine"
	Postgres17 PostgresImage = "debezium/postgres:17-alpine"
)

func SetupPostgresContainer(ctx context.Context, url *string, image PostgresImage, configFile ...string) (cleanup, error) {
	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithWaitStrategy(waitForLogs),
	}
	if len(configFile) > 0 {
		opts = append(opts, postgres.WithConfigFile(configFile[0]))
	}

	ctr, err := postgres.Run(ctx, string(image), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	*url, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("retrieving connection string for postgres container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}
