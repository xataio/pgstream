// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"github.com/eminano/greenmask/pkg/generators"
	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type BooleanTransformer struct {
	transformer *greenmasktransformers.RandomBoolean
}

func NewBooleanTransformer(generatorType transformers.GeneratorType) (*BooleanTransformer, error) {
	t := greenmasktransformers.NewRandomBoolean()
	generator, err := getGreenmaskGenerator(t.GetRequiredGeneratorByteLength(), generatorType)
	if err != nil {
		return nil, err
	}
	generator = generators.NewHashReducer(generator, t.GetRequiredGeneratorByteLength())
	t.SetGenerator(generator)
	return &BooleanTransformer{
		transformer: t,
	}, nil
}

func (bt *BooleanTransformer) Transform(value any) (any, error) {
	var toTransform []byte
	switch val := value.(type) {
	case bool:
		if val {
			toTransform = []byte{1}
		} else {
			toTransform = []byte{0}
		}
	case []byte:
		toTransform = val
	default:
		return nil, transformers.ErrUnsupportedValueType
	}

	ret, err := bt.transformer.Transform(toTransform)
	if err != nil {
		return nil, err
	}
	return bool(ret), nil
}
