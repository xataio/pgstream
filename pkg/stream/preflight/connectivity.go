// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres"
)

// ConnectivityCheck verifies a Postgres URL accepts a connection and answers a
// ping. A connection or ping failure is reported as a finding (not a check
// error), since establishing connectivity is the purpose of the check.
type ConnectivityCheck struct {
	Label string
	URL   string
}

func (c *ConnectivityCheck) Name() string {
	return c.Label + " connectivity"
}

func (c *ConnectivityCheck) Run(ctx context.Context) ([]Finding, error) {
	conn, err := postgres.NewConn(ctx, c.URL)
	if err != nil {
		return []Finding{{Message: fmt.Sprintf("unable to connect: %v", err)}}, nil
	}
	defer conn.Close(ctx)

	if err := conn.Ping(ctx); err != nil {
		return []Finding{{Message: fmt.Sprintf("ping failed: %v", err)}}, nil
	}

	return nil, nil
}
