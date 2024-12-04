// SPDX-License-Identifier: Apache-2.0

package mocks

type Row struct {
	ScanFn func(args ...any) error
}

func (m *Row) Scan(args ...any) error {
	return m.ScanFn(args...)
}
