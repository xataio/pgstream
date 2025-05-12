// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"github.com/xataio/pgstream/pkg/transformers"
)

type TransformerBuilder struct {
	NewFn func(*transformers.Config) (transformers.Transformer, error)
}

func (m *TransformerBuilder) New(cfg *transformers.Config) (transformers.Transformer, error) {
	return m.NewFn(cfg)
}
