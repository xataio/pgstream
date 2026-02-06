// SPDX-License-Identifier: Apache-2.0

package mocks

import natslib "github.com/xataio/pgstream/pkg/nats"

type OffsetParser struct {
	ToStringFn   func(o *natslib.Offset) string
	FromStringFn func(string) (*natslib.Offset, error)
}

func (m *OffsetParser) ToString(o *natslib.Offset) string {
	return m.ToStringFn(o)
}

func (m *OffsetParser) FromString(s string) (*natslib.Offset, error) {
	return m.FromStringFn(s)
}
