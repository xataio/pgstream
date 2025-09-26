// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
)

type LiteralStringTransformer struct {
	literal string
}

var (
	errLiteralStringNotFound     = errors.New("literal_string_transformer: literal parameter not found")
	literalStringCompatibleTypes = []SupportedDataType{
		AllDataTypes,
	}
	literalStringParams = []Parameter{
		{
			Name:          "literal",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
)

func NewLiteralStringTransformer(params ParameterValues) (*LiteralStringTransformer, error) {
	literal, found, err := FindParameter[string](params, "literal")
	if err != nil {
		return nil, fmt.Errorf("literal_string_transformer: literal must be a string: %w", err)
	}
	if !found {
		return nil, errLiteralStringNotFound
	}

	return &LiteralStringTransformer{
		literal: literal,
	}, nil
}

func (lst *LiteralStringTransformer) Transform(_ context.Context, value Value) (any, error) {
	return lst.literal, nil
}

func (lst *LiteralStringTransformer) CompatibleTypes() []SupportedDataType {
	return literalStringCompatibleTypes
}

func (lst *LiteralStringTransformer) Type() TransformerType {
	return LiteralString
}

func (lst *LiteralStringTransformer) Close() error {
	return nil
}

func LiteralStringTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: literalStringCompatibleTypes,
		Parameters:     literalStringParams,
	}
}
