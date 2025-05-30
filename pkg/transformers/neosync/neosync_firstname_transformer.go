// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type FirstNameTransformer struct {
	*transformer[string]
}

var (
	firstNameParams = []transformers.Parameter{
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
	firstNameCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
	}
)

func NewFirstNameTransformer(params transformers.ParameterValues) (*FirstNameTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformFirstNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seed))
	if err != nil {
		return nil, err
	}

	return &FirstNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformFirstName(), opts),
	}, nil
}

func (t *FirstNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return firstNameCompatibleTypes
}

func (t *FirstNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncFirstName
}

func FirstNameTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: firstNameCompatibleTypes,
		Parameters:     firstNameParams,
	}
}
