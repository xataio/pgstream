// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"errors"
	"fmt"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/eminano/greenmask/pkg/toolkit"
	"github.com/xataio/pgstream/pkg/transformers"
)

type ChoiceTransformer struct {
	transformer *greenmasktransformers.RandomChoiceTransformer
}

var errChoicesEmpty = errors.New("greenmask_choice: choices must not be empty")

func NewChoiceTransformer(generatorType transformers.GeneratorType, params transformers.Parameters) (*ChoiceTransformer, error) {
	choices := []string{}
	choices, err := findParameter(params, "choices", choices)
	if err != nil {
		return nil, fmt.Errorf("greenmask_choice: choices must be an array: %w", err)
	}
	if len(choices) == 0 {
		return nil, errChoicesEmpty
	}

	choicesRaw := make([]*toolkit.RawValue, len(choices))
	for i, choice := range choices {
		choicesRaw[i] = &toolkit.RawValue{
			Data:   []byte(choice),
			IsNull: false,
		}
	}

	t := greenmasktransformers.NewRandomChoiceTransformer(choicesRaw)
	if err := setGenerator(t, generatorType); err != nil {
		return nil, err
	}
	return &ChoiceTransformer{
		transformer: t,
	}, nil
}

func (t *ChoiceTransformer) Transform(value any) (any, error) {
	var toTransform []byte
	switch val := value.(type) {
	case []byte:
		toTransform = val
	case string:
		toTransform = []byte(val)
	case *toolkit.RawValue:
		toTransform = val.Data
	default:
		return nil, transformers.ErrUnsupportedValueType
	}
	ret, err := t.transformer.Transform(toTransform)
	if err != nil {
		return nil, err
	}

	return ret.Data, nil
}
