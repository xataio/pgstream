// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

// Both postgres instances are started up front: nearly every test in this
// package needs a source and a target.
//
// Kafka, OpenSearch and Elasticsearch are not. Each serves a single test file,
// and between them they account for most of the package's startup, so they are
// started the first time a test asks for one. Selecting a single postgres test
// no longer waits for three brokers it will never touch.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests exists so that the container cleanups can run before the process
// exits: os.Exit does not run deferred functions, so deferring them in TestMain
// alongside the os.Exit call would leave every container to the testcontainers
// reaper. For the same reason nothing below reaches for log.Fatal.
func runTests(m *testing.M) int {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		return m.Run()
	}

	defer stopContainers()

	if err := setupPostgres(context.Background()); err != nil {
		log.Print(err)
		return 1
	}

	return m.Run()
}

func setupPostgres(ctx context.Context) error {
	pgcleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14, "config/postgresql.conf")
	if err != nil {
		return fmt.Errorf("setting up source postgres: %w", err)
	}
	addContainerCleanup(pgcleanup)

	if err := stream.Init(ctx, &stream.InitConfig{
		PostgresURL:               pgurl,
		InjectorMigrationsEnabled: true,
		MigrationsOnly:            true,
	}); err != nil {
		return fmt.Errorf("initialising pgstream on the source: %w", err)
	}

	targetPGCleanup, err := testcontainers.SetupPostgresContainer(ctx, &targetPGURL, testcontainers.Postgres17)
	if err != nil {
		return fmt.Errorf("setting up target postgres: %w", err)
	}
	addContainerCleanup(targetPGCleanup)

	return nil
}

// lazyContainer starts a container the first time a test asks for it, and keeps
// it for the rest of the run. Its cleanup is registered with the package rather
// than with t.Cleanup, since the container outlives whichever test happened to
// start it.
type lazyContainer struct {
	once  sync.Once
	err   error
	start func(context.Context) (func() error, error)
}

func (c *lazyContainer) require(t *testing.T) {
	t.Helper()

	c.once.Do(func() {
		cleanup, err := c.start(context.Background())
		c.err = err
		if err == nil {
			addContainerCleanup(cleanup)
		}
	})
	require.NoError(t, c.err)
}

var (
	kafkaContainer = &lazyContainer{start: func(ctx context.Context) (func() error, error) {
		return testcontainers.SetupKafkaContainer(ctx, &kafkaBrokers)
	}}
	opensearchContainer = &lazyContainer{start: func(ctx context.Context) (func() error, error) {
		return testcontainers.SetupOpenSearchContainer(ctx, &opensearchURL)
	}}
	elasticsearchContainer = &lazyContainer{start: func(ctx context.Context) (func() error, error) {
		return testcontainers.SetupElasticsearchContainer(ctx, &elasticsearchURL)
	}}
)

var (
	containerMu       sync.Mutex
	containerCleanups []func() error
)

func addContainerCleanup(cleanup func() error) {
	containerMu.Lock()
	defer containerMu.Unlock()
	containerCleanups = append(containerCleanups, cleanup)
}

// stopContainers terminates the containers in the reverse of the order they
// were started. A failure is reported rather than returned: the tests have
// already finished, and the reaper takes care of whatever is left behind.
func stopContainers() {
	containerMu.Lock()
	defer containerMu.Unlock()

	for i := len(containerCleanups) - 1; i >= 0; i-- {
		if err := containerCleanups[i](); err != nil {
			log.Printf("terminating container: %v", err)
		}
	}
	containerCleanups = nil
}
