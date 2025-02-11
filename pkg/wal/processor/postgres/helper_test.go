// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

type mockAdapter struct {
	walEventToQueriesFn func(*wal.Event) ([]*query, error)
}

func (m *mockAdapter) walEventToQueries(_ context.Context, e *wal.Event) ([]*query, error) {
	return m.walEventToQueriesFn(e)
}
