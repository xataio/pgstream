// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"encoding/binary"
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

var floatTransformerParams = []string{"min_value", "max_value", "precision", "generator"}

func NewFloatTransformer(params transformers.Parameters) (*FloatTransformer, error) {
	if err := transformers.ValidateParameters(params, floatTransformerParams); err != nil {
		return nil, err
	}

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

	if err := setGenerator(t, params); err != nil {
		return nil, err
	}

	return &FloatTransformer{
		transformer: t,
	}, nil
}

func (ft *FloatTransformer) Transform(_ context.Context, value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
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

func (ft *FloatTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return []transformers.SupportedDataType{
		transformers.Float32DataType,
		transformers.Float64DataType,
		transformers.ByteArrayDataType,
	}
}

func (ft *FloatTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskFloat
}

func getBytesForFloat(f float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}
