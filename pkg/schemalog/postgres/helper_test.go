// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"

	"github.com/rs/xid"
	"github.com/xataio/pgstream/pkg/schemalog"
)

type mockRow struct {
	logEntry *schemalog.LogEntry
	scanFn   func(args ...any) error
}

func (m *mockRow) Scan(args ...any) error {
	if m.scanFn != nil {
		return m.scanFn(args...)
	}

	id, ok := args[0].(*xid.ID)
	if !ok {
		return fmt.Errorf("unexpected type for xid.ID in scan: %T", args[0])
	}
	*id = m.logEntry.ID

	return nil
}
