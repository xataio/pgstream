// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"

	"github.com/rs/xid"
	"github.com/xataio/pgstream/pkg/schemalog"
)

type mockRow struct {
	logEntry *schemalog.LogEntry
	version  *int
	scanFn   func(args ...any) error
}

func (m *mockRow) Scan(args ...any) error {
	if m.scanFn != nil {
		return m.scanFn(args...)
	}

	if m.logEntry != nil {
		id, ok := args[0].(*xid.ID)
		if !ok {
			return fmt.Errorf("unexpected type for xid.ID in scan: %T", args[0])
		}
		*id = m.logEntry.ID
	}

	if m.version != nil {
		version, ok := args[0].(*int)
		if !ok {
			return fmt.Errorf("unexpected type for version in scan: %T", args[0])
		}
		*version = *m.version
	}

	return nil
}
