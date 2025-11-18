// SPDX-License-Identifier: Apache-2.0

package mocks

import "sync/atomic"

type Bar struct {
	AddFn           func(int) error
	Add64Fn         func(int64) error
	CloseFn         func() error
	CurrentFn       func(uint32) int64
	SetCurrentFn    func(uint32, int64) error
	setCurrentCalls uint32
	closeCalls      uint32
	currentCalls    uint32
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
	atomic.AddUint32(&b.closeCalls, 1)
	if b.CloseFn != nil {
		return b.CloseFn()
	}
	return nil
}

func (b *Bar) Current() int64 {
	atomic.AddUint32(&b.currentCalls, 1)
	if b.CurrentFn != nil {
		return b.CurrentFn(atomic.LoadUint32(&b.currentCalls))
	}
	return 0
}

func (b *Bar) SetCurrent(value int64) error {
	atomic.AddUint32(&b.setCurrentCalls, 1)
	if b.SetCurrentFn != nil {
		return b.SetCurrentFn(atomic.LoadUint32(&b.setCurrentCalls), value)
	}
	return nil
}

func (b *Bar) GetClosedCalls() uint32 {
	return atomic.LoadUint32(&b.closeCalls)
}
