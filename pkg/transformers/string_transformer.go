// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
)

type StringTransformer struct {
	// todo: add buffer pool
	// maxLength int
	// minLength int
}

var (
	stringParams          = []Parameter{}
	stringCompatibleTypes = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
)

func NewStringTransformer(params ParameterValues) (*StringTransformer, error) {
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
	l := int64(len(letterBytes))
	for i := range b {
		a, _ := rand.Int(rand.Reader, big.NewInt(l))
		b[i] = letterBytes[a.Int64()]
	}
	return string(b)
}

func (st *StringTransformer) CompatibleTypes() []SupportedDataType {
	return stringCompatibleTypes
}

func (st *StringTransformer) Type() TransformerType {
	return String
}

func (st *StringTransformer) IsDynamic() bool {
	return false
}

func (st *StringTransformer) Close() error {
	return nil
}

func StringTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: stringCompatibleTypes,
		Parameters:     stringParams,
	}
}
