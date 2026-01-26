// SPDX-License-Identifier: Apache-2.0

package stream

import (
	migratorlib "github.com/xataio/pgstream/internal/migrator"
)

type mockMigrator struct {
	statusFn func() ([]migratorlib.MigrationStatus, error)
	closeFn  func()
}

func (m *mockMigrator) Status() ([]migratorlib.MigrationStatus, error) {
	if m.statusFn != nil {
		return m.statusFn()
	}
	return []migratorlib.MigrationStatus{}, nil
}

func (m *mockMigrator) Close() {
	if m.closeFn != nil {
		m.closeFn()
	}
}
