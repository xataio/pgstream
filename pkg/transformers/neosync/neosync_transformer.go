// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"github.com/xataio/pgstream/pkg/transformers"
)

// transformer is a wrapper around a neosync transformer. Neosync transformers
// return a pointer to the type, so this implementation is generic to ensure
// different types are supported.
type transformer[T any] struct {
	neosyncTransformer neosyncTransformer
	opts               any
}

type neosyncTransformer interface {
	Transform(value any, opts any) (any, error)
}

func New[T any](t neosyncTransformer, opts any) *transformer[T] {
	return &transformer[T]{
		opts:               opts,
		neosyncTransformer: t,
	}
}

func (t *transformer[T]) Transform(value any) (any, error) {
	retPtr, err := t.neosyncTransformer.Transform(value, t.opts)
	if err != nil {
		return nil, err
	}

	ret, ok := retPtr.(*T)
	if !ok {
		return nil, transformers.ErrUnsupportedValueType
	}
	return *ret, nil
}

func findParameter[T any](params transformers.Parameters, name string) (*T, error) {
	var found bool
	var err error

	val := new(T)
	*val, found, err = transformers.FindParameter[T](params, name)
	if err != nil {
		return nil, err
	}
	if !found {
		val = nil
	}
	return val, nil
}

func toInt64Ptr(i *int) *int64 {
	if i == nil {
		return nil
	}

	i64 := int64(*i)
	return &i64
}

func toAnyPtr(strArray *[]string) *any {
	if strArray == nil {
		return nil
	}

	var strArrayAny any
	strArrayAny = *strArray
	return &strArrayAny
}
