// SPDX-License-Identifier: Apache-2.0

package mocks

type Bar struct {
	AddFn   func(int) error
	Add64Fn func(int64) error
	CloseFn func() error
}

func (b *Bar) Add(n int) error {
	if b.AddFn != nil {
		return b.AddFn(n)
	}
	return nil
}

func (b *Bar) Add64(n int64) error {
	if b.Add64Fn != nil {
		return b.Add64Fn(n)
	}
	return nil
}

func (b *Bar) Close() error {
	if b.CloseFn != nil {
		return b.CloseFn()
	}
	return nil
}
