// SPDX-License-Identifier: Apache-2.0

package mocks

type Transformer struct {
	TransformFn func(any) (any, error)
}

func (m *Transformer) Transform(val any) (any, error) {
	return m.TransformFn(val)
}
