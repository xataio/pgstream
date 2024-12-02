// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/opensearch"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xataio/pgstream/pkg/stream"
)

func TestMain(m *testing.M) {
	// if integration tests are not enabled, nothing to setup
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") != "" {
		ctx := context.Background()
		pgcleanup, err := setupPostgresContainer(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer pgcleanup()

		if err := stream.Init(ctx, pgurl, ""); err != nil {
			log.Fatal(err)
		}

		kafkacleanup, err := setupKafkaContainer(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer kafkacleanup()

		oscleanup, err := setupOpenSearchContainer(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer oscleanup()

		escleanup, err := setupElasticsearchContainer(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer escleanup()
	}

	os.Exit(m.Run())
}

type cleanup func() error

func setupPostgresContainer(ctx context.Context) (cleanup, error) {
	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	ctr, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("debezium/postgres:14-alpine"),
		testcontainers.WithWaitStrategy(waitForLogs),
		postgres.WithConfigFile("config/postgresql.conf"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	pgurl, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("retrieving connection string for postgres container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}

func setupKafkaContainer(ctx context.Context) (cleanup, error) {
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

	kafkaBrokers, err = ctr.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving brokers for kafka container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}

func setupOpenSearchContainer(ctx context.Context) (cleanup, error) {
	ctr, err := opensearch.RunContainer(ctx,
		testcontainers.WithImage("opensearchproject/opensearch:2.11.1"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start opensearch container: %w", err)
	}

	opensearchURL, err = ctr.Address(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving url for opensearch container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}

func setupElasticsearchContainer(ctx context.Context) (cleanup, error) {
	ctr, err := elasticsearch.Run(ctx, "docker.elastic.co/elasticsearch/elasticsearch:8.9.0",
		testcontainers.WithEnv(map[string]string{"xpack.security.enabled": "false"})) // disable TLS
	if err != nil {
		return nil, fmt.Errorf("failed to start elasticsearch container: %w", err)
	}

	elasticsearchURL = ctr.Settings.Address

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}
