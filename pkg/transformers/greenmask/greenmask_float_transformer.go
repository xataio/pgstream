// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"fmt"
	"math"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

const (
	defaultMinFloat  = math.MaxFloat32 * -1
	defaultMaxFloat  = math.MaxFloat32
	defaultPrecision = 2
)

type FloatTransformer struct {
	transformer *greenmasktransformers.RandomFloat64Transformer
}

func NewFloatTransformer(generator transformers.GeneratorType, params transformers.Parameters) (*FloatTransformer, error) {
	minValue, err := findParameter(params, "min_value", defaultMinFloat)
	if err != nil {
		return nil, fmt.Errorf("greenmask_float: min_value must be a float: %w", err)
	}
	maxValue, err := findParameter(params, "max_value", defaultMaxFloat)
	if err != nil {
		return nil, fmt.Errorf("greenmask_float: max_value must be a float: %w", err)
	}
	precision, err := findParameter(params, "precision", defaultPrecision)
	if err != nil {
		return nil, fmt.Errorf("greenmask_float: precision must be an integer: %w", err)
	}
	limiter, err := greenmasktransformers.NewFloat64Limiter(minValue, maxValue, precision)
	if err != nil {
		return nil, err
	}
	t := greenmasktransformers.NewRandomFloat64Transformer(limiter)

	if err := setGenerator(t, generator); err != nil {
		return nil, err
	}

	return &FloatTransformer{
		transformer: t,
	}, nil
}

func (ft *FloatTransformer) Transform(value any) (any, error) {
	var toTransform []byte
	switch val := value.(type) {
	case float32:
		toTransform = getBytesForFloat(float64(val))
	case float64:
		toTransform = getBytesForFloat(val)
	case []byte:
		toTransform = val
	default:
		return nil, transformers.ErrUnsupportedValueType
	}
	ret, err := ft.transformer.Transform(nil, toTransform)
	if err != nil {
		return nil, err
	}
	return float64(ret), nil
}

func getBytesForFloat(f float64) []byte {
	var buf [8]byte
	n := math.Float64bits(f)
	buf[0] = byte(n >> 56)
	buf[1] = byte(n >> 48)
	buf[2] = byte(n >> 40)
	buf[3] = byte(n >> 32)
	buf[4] = byte(n >> 24)
	buf[5] = byte(n >> 16)
	buf[6] = byte(n >> 8)
	buf[7] = byte(n)
	return buf[:]
}
