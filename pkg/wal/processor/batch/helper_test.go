// SPDX-License-Identifier: Apache-2.0

package batch

type mockMessage struct {
	id        uint
	isEmptyFn func() bool
	sizeFn    func() int
}

func (m *mockMessage) Size() int {
	if m.sizeFn != nil {
		return m.sizeFn()
	}
	return 1
}

func (m *mockMessage) IsEmpty() bool {
	if m.isEmptyFn != nil {
		return m.isEmptyFn()
	}
	return false
}
