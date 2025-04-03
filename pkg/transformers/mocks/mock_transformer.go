// SPDX-License-Identifier: Apache-2.0

package mocks

import "github.com/xataio/pgstream/pkg/transformers"

type Transformer struct {
	TransformFn func(transformers.Value) (any, error)
}

func (m *Transformer) Transform(val transformers.Value) (any, error) {
	return m.TransformFn(val)
}
