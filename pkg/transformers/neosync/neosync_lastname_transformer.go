// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type LastNameTransformer struct {
	*transformer[string]
}

var (
	lastNameParams = []transformers.Parameter{
		{
			Name:          "seed",
			SupportedType: "int",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "preserve_length",
			SupportedType: "boolean",
			Default:       false,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "max_length",
			SupportedType: "int",
			Default:       100,
			Dynamic:       false,
			Required:      false,
		},
	}
	lastNameCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
	}
)

func NewLastNameTransformer(params transformers.ParameterValues) (*LastNameTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformLastNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seed))
	if err != nil {
		return nil, err
	}

	return &LastNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformLastName(), opts),
	}, nil
}

func (t *LastNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return lastNameCompatibleTypes
}

func (t *LastNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncLastName
}

func LastNameTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: lastNameCompatibleTypes,
		Parameters:     lastNameParams,
	}
}
