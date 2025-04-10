// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/google/uuid"
	"github.com/xataio/pgstream/pkg/transformers"
)

type UUIDTransformer struct {
	transformer *greenmasktransformers.RandomUuidTransformer
}

var UUIDTransformerParams = []string{"generator"}

func NewUUIDTransformer(params transformers.Parameters) (*UUIDTransformer, error) {
	t := greenmasktransformers.NewRandomUuidTransformer()
	if err := setGenerator(t, params); err != nil {
		return nil, err
	}
	return &UUIDTransformer{
		transformer: t,
	}, nil
}

func (ut *UUIDTransformer) Transform(value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
	case string:
		parsed, err := uuid.Parse(val)
		if err != nil {
			return nil, err
		}
		toTransform = parsed[:]
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
	return []transformers.SupportedDataType{
		transformers.StringDataType,
		transformers.UUIDDataType,
		transformers.ByteArrayDataType,
		transformers.UInt8ArrayOf16DataType,
	}
}
