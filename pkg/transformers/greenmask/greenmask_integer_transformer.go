// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"errors"
	"fmt"

	"github.com/eminano/greenmask/pkg/generators"
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

const defaultSize = 4

type IntegerTransformer struct {
	transformer *greenmasktransformers.RandomInt64Transformer
}

var (
	ErrUnsupportedSizeError = errors.New("greenmask_integer: size must be between 1 and 8")
)

// NewIntegerTransformer creates a new IntegerTransformer with the specified generator and parameters.
// The size parameter must be between 1 and 8 (inclusive), and the min_value and max_value parameters
// must be valid integers within the range of the specified size.
func NewIntegerTransformer(generator transformers.GeneratorType, params transformers.Parameters) (*IntegerTransformer, error) {
	size, err := findParameter(params, "size", int(defaultSize))
	if err != nil {
		return nil, fmt.Errorf("greenmask_integer: size must be an integer: %w", err)
	}
	if size < 1 || size > 8 {
		return nil, ErrUnsupportedSizeError
	}

	defaultMinValue := minValueForSize(size)
	defaultMaxValue := maxValueForSize(size)

	minValue, err := findParameter(params, "min_value", defaultMinValue)
	if err != nil {
		return nil, fmt.Errorf("greenmask_integer: min_value must be an integer: %w", err)
	}
	maxValue, err := findParameter(params, "max_value", defaultMaxValue)
	if err != nil {
		return nil, fmt.Errorf("greenmask_integer: max_value must be an integer: %w", err)
	}
	limiter, err := greenmasktransformers.NewInt64Limiter(minValue, maxValue)
	if err != nil {
		return nil, err
	}

	t, _ := greenmasktransformers.NewRandomInt64Transformer(limiter, size)

	if err := setGenerator(t, generator); err != nil {
		return nil, err
	}

	return &IntegerTransformer{
		transformer: t,
	}, nil
}

// Transform converts the input value to a byte slice, passes it through the underlying
// RandomInt64Transformer, and returns the transformed value as an int64.
// Supported input types are int, int8, int16, int32, int64, uint8, uint16, and uint32.
// If the input value is a byte slice, it is passed through the transformer without modification.
// If the input value is of an unsupported type, an error is returned.
func (t *IntegerTransformer) Transform(value any) (any, error) {
	var toTransform []byte
	switch val := value.(type) {
	case int:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case int8:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case int16:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case int32:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case int64:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case uint:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case uint8:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case uint16:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case uint32:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case uint64:
		toTransform = generators.BuildBytesFromUint64(uint64(val))
	case []byte:
		toTransform = val
	default:
		return nil, transformers.ErrUnsupportedValueType
	}

	ret, err := t.transformer.Transform(nil, toTransform)
	if err != nil {
		return nil, err
	}

	return int64(ret), nil
}

func minValueForSize(size int) int64 {
	return int64(-1 << (size*8 - 1))
}

func maxValueForSize(size int) int64 {
	return int64((1 << (size*8 - 1)) - 1)
}
