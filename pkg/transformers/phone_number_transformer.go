// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/pkg/transformers/generators"
)

type PhoneNumberTransformer struct {
	prefix        string
	maxLength     int
	minLength     int
	generator     generators.Generator
	dynamicParams map[string]*DynamicParameter
}

var (
	PhoneNumberCompatibleTypes = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}

	PhoneNumberParams = []Parameter{
		{
			Name:          "prefix",
			SupportedType: "string",
			Default:       "",
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "min_length",
			SupportedType: "int",
			Default:       6,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "max_length",
			SupportedType: "int",
			Default:       10,
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

const prefixParam = "prefix"

func NewPhoneNumberTransformer(params, dynamicParams ParameterValues) (*PhoneNumberTransformer, error) {
	prefix, err := FindParameterWithDefault(params, "prefix", "")
	if err != nil {
		return nil, fmt.Errorf("phone_number: prefix must be a string: %w", err)
	}

	dynamicParamMap, err := ParseDynamicParameters(dynamicParams)
	if err != nil {
		return nil, err
	}

	maxLength, err := FindParameterWithDefault(params, "max_length", 10)
	if err != nil {
		return nil, fmt.Errorf("phone_number: max_length must be an integer: %w", err)
	}

	minLength, err := FindParameterWithDefault(params, "min_length", 6)
	if err != nil {
		return nil, fmt.Errorf("phone_number: min_length must be an integer: %w", err)
	}

	if minLength < 0 {
		return nil, fmt.Errorf("phone_number: min_length must be greater than 0")
	}

	if maxLength < minLength {
		return nil, fmt.Errorf("phone_number: max_length must be greater than min_length")
	}

	if len(prefix) > minLength {
		return nil, fmt.Errorf("phone_number: prefix must be less than min_length")
	}

	generatorType, _, err := FindParameter[string](params, "generator")
	if err != nil {
		return nil, fmt.Errorf("phone_number: generator must be a string: %w", err)
	}
	var generator generators.Generator
	if generatorType == "deterministic" {
		// Add an extra byte to be used for randomizing the output length as well
		generator, err = generators.NewDeterministicBytesGenerator(maxLength + 1)
		if err != nil {
			return nil, fmt.Errorf("phone_number: error creating deterministic generator: %w", err)
		}
	} else {
		generator = generators.NewRandomBytesGenerator(maxLength + 1)
	}

	return &PhoneNumberTransformer{
		prefix:        prefix,
		maxLength:     maxLength,
		minLength:     minLength,
		generator:     generator,
		dynamicParams: dynamicParamMap,
	}, nil
}

func (t *PhoneNumberTransformer) Transform(_ context.Context, value Value) (any, error) {
	switch v := value.TransformValue.(type) {
	case string:
		return t.transform([]byte(v), value.DynamicValues)
	case []byte:
		return t.transform(v, value.DynamicValues)
	default:
		return nil, ErrUnsupportedValueType
	}
}

func (t *PhoneNumberTransformer) transform(value []byte, dynamicValues map[string]any) (string, error) {
	const letterBytes = "0123456789"

	data, err := t.generator.Generate(value)
	if err != nil {
		return "", err
	}

	// Generate random length between min and max (accounting for prefix)
	targetLen := t.minLength
	firstByte, data := data[0], data[1:] // Remove the first byte used for length calculation
	if t.maxLength > t.minLength {
		targetLen += int(firstByte) % (t.maxLength - t.minLength + 1)
	}

	prefix := t.prefix
	if prefixDynamicParam := t.dynamicParams[prefixParam]; prefixDynamicParam != nil {
		var err error
		prefix, err = FindDynamicValue(prefixDynamicParam, dynamicValues, prefix)
		if err != nil {
			return "", fmt.Errorf("invalid value type for dynamic parameter %q", prefixParam)
		}
	}

	b := make([]byte, targetLen)
	// Add prefix
	prefixLen := len(prefix)
	remainingLen := targetLen - prefixLen
	if remainingLen > len(data) {
		return "", fmt.Errorf("phone_number: generated data not enough for target length")
	}
	copy(b[:prefixLen], prefix)

	// Fill remaining space with random digits
	for i := 0; i < remainingLen; i++ {
		b[prefixLen+i] = letterBytes[int(data[i])%len(letterBytes)]
	}

	return string(b[:targetLen]), nil
}

func (t *PhoneNumberTransformer) CompatibleTypes() []SupportedDataType {
	return PhoneNumberCompatibleTypes
}

func (t *PhoneNumberTransformer) Type() TransformerType {
	return PhoneNumber
}
