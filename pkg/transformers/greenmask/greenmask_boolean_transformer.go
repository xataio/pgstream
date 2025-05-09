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

var booleanTransformerParams = []string{"generator"}

func NewBooleanTransformer(params transformers.Parameters) (*BooleanTransformer, error) {
	if err := transformers.ValidateParameters(params, booleanTransformerParams); err != nil {
		return nil, err
	}
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
	return []transformers.SupportedDataType{
		transformers.BooleanDataType,
		transformers.ByteArrayDataType,
	}
}

func (bt *BooleanTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskBoolean
}
