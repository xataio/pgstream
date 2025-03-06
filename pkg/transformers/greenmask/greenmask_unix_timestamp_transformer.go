// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"errors"
	"fmt"

	"github.com/xataio/pgstream/pkg/transformers"
)

var errMinMaxValueNotSpecified = errors.New("greenmask_unix_timestamp: min_value and max_value must be specified")

func NewUnixTimestampTransformer(generatorType transformers.GeneratorType, params transformers.Parameters) (*IntegerTransformer, error) {
	minValue, foundMin, err := transformers.FindParameter[int64](params, "min_value")
	if err != nil {
		return nil, fmt.Errorf("greenmask_unix_timestamp: min_value must be an int64: %w", err)
	}

	maxValue, foundMax, err := transformers.FindParameter[int64](params, "max_value")
	if err != nil {
		return nil, fmt.Errorf("greenmask_unix_timestamp: max_value must be an int64: %w", err)
	}

	if !foundMin || !foundMax {
		return nil, errMinMaxValueNotSpecified
	}

	transformer, err := NewIntegerTransformer(generatorType, transformers.Parameters{
		"size":      8,
		"min_value": minValue,
		"max_value": maxValue,
	})
	if err != nil {
		return nil, err
	}

	return transformer, nil
}
