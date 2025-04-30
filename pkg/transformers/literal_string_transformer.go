// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"errors"
	"fmt"
)

type LiteralStringTransformer struct {
	literal string
}

var (
	literalStringTransformerParams = []string{"literal"}
	errLiteralStringCannotBeEmpty  = errors.New("literal_string_transformer: literal parameter cannot be empty")
)

func NewLiteralStringTransformer(params Parameters) (*LiteralStringTransformer, error) {
	if err := ValidateParameters(params, literalStringTransformerParams); err != nil {
		return nil, err
	}

	literal, _, err := FindParameter[string](params, "literal")
	if err != nil {
		return nil, fmt.Errorf("literal_string_transformer: literal must be a string: %w", err)
	}
	if literal == "" {
		return nil, errLiteralStringCannotBeEmpty
	}

	return &LiteralStringTransformer{
		literal: literal,
	}, nil
}

func (lst *LiteralStringTransformer) Transform(value Value) (any, error) {
	return lst.literal, nil
}

func (lst *LiteralStringTransformer) CompatibleTypes() []SupportedDataType {
	return []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
		JSONDataType,
	}
}
