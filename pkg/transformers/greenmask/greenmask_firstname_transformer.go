// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"fmt"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type FirstNameTransformer struct {
	transformer   *greenmasktransformers.RandomPersonTransformer
	dynamicParams map[string]*transformers.DynamicParameter
}

const genderParam = "gender"

var FirstNameTransformerParams = []string{genderParam, "generator"}

func NewFirstNameTransformer(params, dynamicParams transformers.Parameters) (*FirstNameTransformer, error) {
	gender, err := transformers.FindParameterWithDefault(params, genderParam, greenmasktransformers.AnyGenderName)
	if err != nil {
		return nil, fmt.Errorf("greenmask_firstname: gender must be a string: %w", err)
	}

	dynamicParamMap, err := transformers.ParseDynamicParameters(dynamicParams)
	if err != nil {
		return nil, err
	}

	t := greenmasktransformers.NewRandomPersonTransformer(toGreenmaskGender(gender), nil)
	if err := setGenerator(t, params); err != nil {
		return nil, err
	}

	return &FirstNameTransformer{
		transformer:   t,
		dynamicParams: dynamicParamMap,
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

	gender := ""
	// If there's a dynamic parameter defined for the gender, get the value and
	// pass it to the transformer
	if genderDynamicParam := fnt.dynamicParams[genderParam]; genderDynamicParam != nil {
		var err error
		gender, err = transformers.FindDynamicValue(genderDynamicParam, value.DynamicValues, gender)
		if err != nil {
			return nil, fmt.Errorf("invalid value type for dynamic parameter %q", genderParam)
		}
	}

	ret, err := fnt.transformer.GetFullName(gender, toTransform)
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

func (fnt *FirstNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return []transformers.SupportedDataType{
		transformers.StringDataType,
		transformers.ByteArrayDataType,
	}
}
