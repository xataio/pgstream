// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/transformers"
)

type Transformer struct {
	TransformFn       func(transformers.Value) (any, error)
	IsDynamicFn       func() bool
	CompatibleTypesFn func() []transformers.SupportedDataType
	UniquenessFn      func() transformers.Uniqueness
}

func (m *Transformer) Transform(_ context.Context, val transformers.Value) (any, error) {
	return m.TransformFn(val)
}

func (m *Transformer) CompatibleTypes() []transformers.SupportedDataType {
	return m.CompatibleTypesFn()
}

func (m *Transformer) Type() transformers.TransformerType {
	return transformers.TransformerType("mock")
}

func (m *Transformer) IsDynamic() bool {
	if m.IsDynamicFn != nil {
		return m.IsDynamicFn()
	}
	return false
}

func (m *Transformer) Uniqueness() transformers.Uniqueness {
	if m.UniquenessFn != nil {
		return m.UniquenessFn()
	}
	return transformers.UniquenessPreserved
}

func (m *Transformer) Close() error {
	return nil
}
