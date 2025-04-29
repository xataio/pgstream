// SPDX-License-Identifier: Apache-2.0

package stream

type mockMigrator struct {
	versionFn func() (uint, bool, error)
	closeFn   func() (error, error)
}

func (m *mockMigrator) Version() (uint, bool, error) {
	if m.versionFn != nil {
		return m.versionFn()
	}
	return 0, false, nil
}

func (m *mockMigrator) Close() (error, error) {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil, nil
}
