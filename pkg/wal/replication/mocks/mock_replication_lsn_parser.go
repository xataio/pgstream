// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"github.com/xataio/pgstream/pkg/wal/replication"
)

type LSNParser struct {
	ToStringFn   func(replication.LSN) string
	FromStringFn func(string) (replication.LSN, error)
}

func (m *LSNParser) ToString(lsn replication.LSN) string {
	return m.ToStringFn(lsn)
}

func (m *LSNParser) FromString(lsn string) (replication.LSN, error) {
	return m.FromStringFn(lsn)
}
