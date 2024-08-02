// SPDX-License-Identifier: Apache-2.0

package mocks

import "github.com/xataio/pgstream/pkg/kafka"

type OffsetParser struct {
	ToStringFn   func(o *kafka.Offset) string
	FromStringFn func(string) (*kafka.Offset, error)
}

func (m *OffsetParser) ToString(o *kafka.Offset) string {
	return m.ToStringFn(o)
}

func (m *OffsetParser) FromString(s string) (*kafka.Offset, error) {
	return m.FromStringFn(s)
}
