// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"errors"
	"fmt"
	"strconv"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

var (
	errMinMaxValueNotSpecified = errors.New("min_value and max_value must be specified")
	unixTimestampParams        = []transformers.Parameter{
		{
			Name:          "generator",
			SupportedType: "string",
			Default:       "random",
			Dynamic:       false,
			Required:      false,
			Values:        []any{"random", "deterministic"},
		},
		{
			Name:          "min_value",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "max_value",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
	unixTimestampCompatibleTypes = integerCompatibleTypes
)

type UnixTimestampTransformer struct {
	*IntegerTransformer
}

func NewUnixTimestampTransformer(params transformers.ParameterValues) (*UnixTimestampTransformer, error) {
	minValueStr, foundMin, err := transformers.FindParameter[string](params, "min_value")
	if err != nil {
		return nil, fmt.Errorf("greenmask_unix_timestamp: min_value must be a string: %w", err)
	}

	maxValueStr, foundMax, err := transformers.FindParameter[string](params, "max_value")
	if err != nil {
		return nil, fmt.Errorf("greenmask_unix_timestamp: max_value must be a string: %w", err)
	}

	if !foundMin || !foundMax {
		return nil, errMinMaxValueNotSpecified
	}

	minValue, err := strconv.ParseInt(minValueStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("greenmask_unix_timestamp: min_value cannot be parsed into int64: %w", err)
	}

	maxValue, err := strconv.ParseInt(maxValueStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("greenmask_unix_timestamp: max_value cannot be parsed into int64: %w", err)
	}

	limiter, err := greenmasktransformers.NewInt64Limiter(int64(minValue), int64(maxValue))
	if err != nil {
		return nil, err
	}

	t, err := greenmasktransformers.NewRandomInt64Transformer(limiter, 8)
	if err != nil {
		return nil, err
	}

	if err := setGenerator(t, params); err != nil {
		return nil, err
	}

	return &UnixTimestampTransformer{
		IntegerTransformer: &IntegerTransformer{
			transformer: t,
		},
	}, nil
}

func (t *UnixTimestampTransformer) PostCreate(param any) error {
	return nil
}

func (t *UnixTimestampTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskUnixTimestamp
}

func UnixTimestampTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: unixTimestampCompatibleTypes,
		Parameters:     unixTimestampParams,
	}
}
