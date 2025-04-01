// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go/modules/opensearch"
)

const opensearchImage = "opensearchproject/opensearch:2.11.1"

func SetupOpenSearchContainer(ctx context.Context, url *string) (cleanup, error) {
	ctr, err := opensearch.Run(ctx, opensearchImage)
	if err != nil {
		return nil, fmt.Errorf("failed to start opensearch container: %w", err)
	}

	*url, err = ctr.Address(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving url for opensearch container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}
