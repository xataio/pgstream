// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"fmt"

	"github.com/xataio/pgstream/pkg/transformers/generators"
)

type PhoneNumberTransformer struct {
	prefix    string
	maxLength int
	minLength int
	generator generators.Generator
}

func NewPhoneNumberTransformer(params Parameters) (*PhoneNumberTransformer, error) {
	prefix, found, err := FindParameter[string](params, "prefix")
	if err != nil {
		return nil, fmt.Errorf("phone_number: prefix must be a string: %w", err)
	}
	if !found {
		prefix = ""
	}

	maxLength, found, err := FindParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("phone_number: max_length must be an integer: %w", err)
	}
	if !found {
		maxLength = 10
	}

	minLength, found, err := FindParameter[int](params, "min_length")
	if err != nil {
		return nil, fmt.Errorf("phone_number: min_length must be an integer: %w", err)
	}
	if !found {
		minLength = 6
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
		generator, err = generators.NewDeterministicBytesGenerator(maxLength)
		if err != nil {
			return nil, fmt.Errorf("phone_number: error creating deterministic generator: %w", err)
		}
	} else {
		generator = generators.NewRandomBytesGenerator(maxLength)
	}

	return &PhoneNumberTransformer{
		prefix:    prefix,
		maxLength: maxLength,
		minLength: minLength,
		generator: generator,
	}, nil
}

func (t *PhoneNumberTransformer) Transform(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return t.transform([]byte(v))
	case []byte:
		return t.transform(v)
	default:
		return nil, ErrUnsupportedValueType
	}
}

func (t *PhoneNumberTransformer) transform(value []byte) (string, error) {
	const letterBytes = "0123456789"

	data, err := t.generator.Generate(value)
	if err != nil {
		return "", err
	}

	// Generate random length between min and max (accounting for prefix)
	targetLen := t.minLength
	if t.maxLength > t.minLength {
		targetLen += int(data[0]) % (t.maxLength - t.minLength + 1)
	}

	b := make([]byte, targetLen)
	// Add prefix
	prefixLen := len(t.prefix)
	copy(b[:prefixLen], t.prefix)

	// Fill remaining space with random digits
	for i := prefixLen; i < targetLen; i++ {
		b[i] = letterBytes[int(data[i])%len(letterBytes)]
	}

	return string(b[:targetLen]), nil
}
