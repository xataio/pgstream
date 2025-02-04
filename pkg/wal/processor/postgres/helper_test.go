// SPDX-License-Identifier: Apache-2.0

package postgres

import "github.com/xataio/pgstream/pkg/wal"

type mockAdapter struct {
	walEventToQueryFn func(*wal.Event) (*query, error)
}

func (m *mockAdapter) walEventToQuery(e *wal.Event) (*query, error) {
	return m.walEventToQueryFn(e)
}
