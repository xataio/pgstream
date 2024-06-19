// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"github.com/jackc/pglogrepl"

	"github.com/xataio/pgstream/pkg/wal/replication"
)

// LSNParser is the postgres implementation of the replication.LSNParser
type LSNParser struct{}

func NewLSNParser() *LSNParser {
	return &LSNParser{}
}

func (p *LSNParser) FromString(lsnStr string) (replication.LSN, error) {
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, err
	}
	return replication.LSN(lsn), nil
}

func (p *LSNParser) ToString(lsn replication.LSN) string {
	return pglogrepl.LSN(lsn).String()
}
