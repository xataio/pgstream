// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Rows struct {
	CloseFn             func()
	ErrFn               func() error
	FieldDescriptionsFn func() []pgconn.FieldDescription
	NextFn              func(i uint) bool
	ScanFn              func(i uint, dest ...any) error
	ValuesFn            func() ([]any, error)
	RawValuesFn         func() [][]byte
	nextCalls           uint
	scanCalls           uint
}

func (m *Rows) Close() {
	if m.CloseFn == nil {
		return
	}
	m.CloseFn()
}

func (m *Rows) Err() error {
	return m.ErrFn()
}

func (m *Rows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (m *Rows) FieldDescriptions() []pgconn.FieldDescription {
	return m.FieldDescriptionsFn()
}

func (m *Rows) Next() bool {
	m.nextCalls++
	return m.NextFn(m.nextCalls)
}

func (m *Rows) Scan(dest ...any) error {
	m.scanCalls++
	return m.ScanFn(m.scanCalls, dest...)
}

func (m *Rows) Values() ([]any, error) {
	return m.ValuesFn()
}

func (m *Rows) RawValues() [][]byte {
	if m.RawValuesFn == nil {
		return nil
	}
	return m.RawValuesFn()
}

func (m *Rows) Conn() *pgx.Conn {
	return &pgx.Conn{}
}
