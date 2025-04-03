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

func NewFirstNameTransformer(params transformers.Parameters) (*FirstNameTransformer, error) {
	gender, err := findParameter(params, "gender", greenmasktransformers.AnyGenderName)
	if err != nil {
		return nil, fmt.Errorf("greenmask_string: symbols must be a string: %w", err)
	}

	t := greenmasktransformers.NewRandomPersonTransformer(toGreenmaskGender(gender), nil)

	if err := setGenerator(t, params); err != nil {
		return nil, err
	}

	return &FirstNameTransformer{
		transformer: t,
	}, nil
}

func (fnt *FirstNameTransformer) Transform(value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
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
