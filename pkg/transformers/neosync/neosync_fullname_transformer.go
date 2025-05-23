// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type FullNameTransformer struct {
	*transformer[string]
}

var (
	FullNameParams = []transformers.TransformerParameter{
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
	FullNameCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
	}
)

func NewFullNameTransformer(params transformers.Parameters) (*FullNameTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_fullname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_fullname: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_fullname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformFullNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seed))
	if err != nil {
		return nil, err
	}

	return &FullNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformFullName(), opts),
	}, nil
}

func (t *FullNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return FullNameCompatibleTypes
}

func (t *FullNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncFullName
}
