// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"fmt"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type StringTransformer struct {
	transformer *greenmasktransformers.RandomStringTransformer
}

const defaultSymbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

var (
	stringCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
		transformers.ByteArrayDataType,
	}

	stringParams = []transformers.Parameter{
		{
			Name:          "symbols",
			SupportedType: "string",
			Default:       defaultSymbols,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "min_length",
			SupportedType: "int",
			Default:       1,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "max_length",
			SupportedType: "int",
			Default:       100,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "generator",
			SupportedType: "string",
			Default:       "random",
			Dynamic:       false,
			Required:      false,
			Values:        []any{"random", "deterministic"},
		},
	}
)

func NewStringTransformer(params transformers.ParameterValues) (*StringTransformer, error) {
	symbols, err := findParameter(params, "symbols", defaultSymbols)
	if err != nil {
		return nil, fmt.Errorf("greenmask_string: symbols must be a string: %w", err)
	}

	minLength, err := findParameter(params, "min_length", 1)
	if err != nil {
		return nil, fmt.Errorf("greenmask_string: min_length must be an integer: %w", err)
	}

	maxLength, err := findParameter(params, "max_length", 100)
	if err != nil {
		return nil, fmt.Errorf("greenmask_string: max_length must be an integer: %w", err)
	}

	t, err := greenmasktransformers.NewRandomStringTransformer([]rune(symbols), minLength, maxLength)
	if err != nil {
		return nil, err
	}

	if err := setGenerator(t, params); err != nil {
		return nil, err
	}

	return &StringTransformer{
		transformer: t,
	}, nil
}

func (st *StringTransformer) Transform(_ context.Context, value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
	case string:
		toTransform = []byte(val)
	case []byte:
		toTransform = val
	default:
		return nil, transformers.ErrUnsupportedValueType
	}

	ret := st.transformer.Transform(toTransform)
	return string(ret), nil
}

func (st *StringTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return stringCompatibleTypes
}

func (st *StringTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskString
}

func (st *StringTransformer) Close() error {
	return nil
}

func StringTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: stringCompatibleTypes,
		Parameters:     stringParams,
	}
}
