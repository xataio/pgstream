// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"fmt"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type FirstNameTransformer struct {
	transformer *greenmasktransformers.RandomPersonTransformer
}

func NewFirstNameTransformer(generatorType transformers.GeneratorType, params transformers.Parameters) (*FirstNameTransformer, error) {
	gender, err := findParameter(params, "gender", greenmasktransformers.AnyGenderName)
	if err != nil {
		return nil, fmt.Errorf("greenmask_string: symbols must be a string: %w", err)
	}

	t := greenmasktransformers.NewRandomPersonTransformer(toGreenmaskGender(gender), nil)

	generator, err := getGreenmaskGenerator(t.GetRequiredGeneratorByteLength(), generatorType)
	if err != nil {
		return nil, err
	}
	t.SetGenerator(generator)

	return &FirstNameTransformer{
		transformer: t,
	}, nil
}

func (fnt *FirstNameTransformer) Transform(value any) (any, error) {
	var toTransform []byte
	switch val := value.(type) {
	case string:
		toTransform = []byte(val)
	case []byte:
		toTransform = val
	default:
		return nil, transformers.ErrUnsupportedValueType
	}

	ret, err := fnt.transformer.GetFullName("", toTransform)
	if err != nil {
		return nil, err
	}

	return ret["FirstName"], nil
}

func toGreenmaskGender(gender string) string {
	switch gender {
	case "female":
		return greenmasktransformers.FemaleGenderName
	case "male":
		return greenmasktransformers.MaleGenderName
	default:
		return greenmasktransformers.AnyGenderName
	}
}
