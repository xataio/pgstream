// SPDX-License-Identifier: Apache-2.0

package mocks

type Row struct {
	ScanFn    func(i uint, args ...any) error
	scanCalls uint
}

func (m *Row) Scan(args ...any) error {
	m.scanCalls++
	return m.ScanFn(m.scanCalls, args...)
}
