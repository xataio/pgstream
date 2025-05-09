// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"fmt"

	"golang.org/x/exp/rand"
)

type StringTransformer struct {
	// todo: add buffer pool
	// maxLength int
	// minLength int
}

var stringTransformerParams = []string{}

func NewStringTransformer(params Parameters) (*StringTransformer, error) {
	if err := ValidateParameters(params, stringTransformerParams); err != nil {
		return nil, err
	}
	return &StringTransformer{}, nil
}

func (st *StringTransformer) Transform(_ context.Context, v Value) (any, error) {
	switch str := v.TransformValue.(type) {
	case string:
		return st.transform(str), nil
	case []byte:
		return st.transform(string(str)), nil
	default:
		return v, fmt.Errorf("expected string, got %T: %w", v, ErrUnsupportedValueType)
	}
}

func (st *StringTransformer) transform(str string) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, len(str))
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (st *StringTransformer) CompatibleTypes() []SupportedDataType {
	return []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
}
