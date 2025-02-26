// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"github.com/eminano/greenmask/pkg/generators"
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/google/uuid"
	"github.com/xataio/pgstream/pkg/transformers"
)

type UUIDTransformer struct {
	transformer *greenmasktransformers.RandomUuidTransformer
}

func NewUUIDTransformer(generatorType transformers.GeneratorType) (*UUIDTransformer, error) {
	t := greenmasktransformers.NewRandomUuidTransformer()
	generator, err := getGreenmaskGenerator(t.GetRequiredGeneratorByteLength(), generatorType)
	if err != nil {
		return nil, err
	}
	generator = generators.NewHashReducer(generator, t.GetRequiredGeneratorByteLength())
	t.SetGenerator(generator)
	return &UUIDTransformer{
		transformer: t,
	}, nil
}

func (ut *UUIDTransformer) Transform(value any) (any, error) {
	var toTransform []byte
	switch val := value.(type) {
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
	default:
		return nil, transformers.ErrUnsupportedValueType
	}
	ret, err := ut.transformer.Transform(toTransform)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
