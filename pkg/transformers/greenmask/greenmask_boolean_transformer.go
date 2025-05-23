// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type BooleanTransformer struct {
	transformer *greenmasktransformers.RandomBoolean
}

var (
	BooleanParams = []transformers.TransformerParameter{
		{
			Name:          "generator",
			SupportedType: "string",
			Default:       "random",
			Dynamic:       false,
			Required:      false,
			Values:        []any{"random", "deterministic"},
		},
	}
	BooleanCompatibleTypes = []transformers.SupportedDataType{
		transformers.BooleanDataType,
		transformers.ByteArrayDataType,
	}
)

func NewBooleanTransformer(params transformers.Parameters) (*BooleanTransformer, error) {
	t := greenmasktransformers.NewRandomBoolean()
	if err := setGenerator(t, params); err != nil {
		return nil, err
	}
	return &BooleanTransformer{
		transformer: t,
	}, nil
}

func (bt *BooleanTransformer) Transform(_ context.Context, value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
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

func (bt *BooleanTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return BooleanCompatibleTypes
}

func (bt *BooleanTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskBoolean
}
