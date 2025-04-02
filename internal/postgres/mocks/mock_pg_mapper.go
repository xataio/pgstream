// SPDX-License-Identifier: Apache-2.0

package mocks

type Mapper struct {
	TypeForOIDFn func(oid uint32) (string, error)
}

func (m *Mapper) TypeForOID(oid uint32) (string, error) {
	return m.TypeForOIDFn(oid)
}
