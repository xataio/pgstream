// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/google/uuid"
	"github.com/xataio/pgstream/pkg/transformers"
)

type UUIDTransformer struct {
	transformer *greenmasktransformers.RandomUuidTransformer
}

var (
	uuidParams = []transformers.Parameter{
		{
			Name:          "generator",
			SupportedType: "string",
			Default:       "random",
			Dynamic:       false,
			Required:      false,
			Values:        []any{"random", "deterministic"},
		},
	}
	uuidCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
		transformers.ByteArrayDataType,
		transformers.UUIDDataType,
		transformers.UInt8ArrayOf16DataType,
	}
)

func NewUUIDTransformer(params transformers.ParameterValues) (*UUIDTransformer, error) {
	t := greenmasktransformers.NewRandomUuidTransformer()
	if err := setGenerator(t, params); err != nil {
		return nil, err
	}
	return &UUIDTransformer{
		transformer: t,
	}, nil
}

func (ut *UUIDTransformer) Transform(_ context.Context, value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
	case string:
		toTransform = []byte(val)
	case uuid.UUID:
		toTransform = val[:]
	case []byte:
		toTransform = val
	case [16]uint8:
		toTransform = val[:]
	default:
		return nil, transformers.ErrUnsupportedValueType
	}
	ret, err := ut.transformer.Transform(toTransform)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (ut *UUIDTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return uuidCompatibleTypes
}

func (ut *UUIDTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskUUID
}

func (ut *UUIDTransformer) Close() error {
	return nil
}

func UUIDTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: uuidCompatibleTypes,
		Parameters:     uuidParams,
	}
}
