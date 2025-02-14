// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/elasticsearch"
)

func SetupElasticsearchContainer(ctx context.Context, url *string) (cleanup, error) {
	ctr, err := elasticsearch.Run(ctx, "docker.elastic.co/elasticsearch/elasticsearch:8.9.0",
		testcontainers.WithEnv(map[string]string{"xpack.security.enabled": "false"})) // disable TLS
	if err != nil {
		return nil, fmt.Errorf("failed to start elasticsearch container: %w", err)
	}

	*url = ctr.Settings.Address

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}
