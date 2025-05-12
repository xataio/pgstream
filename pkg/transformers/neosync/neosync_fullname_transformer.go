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

var fullNameTransformerParams = []string{"preserve_length", "max_length", "seed"}

func NewFullNameTransformer(params transformers.Parameters) (*FullNameTransformer, error) {
	if err := transformers.ValidateParameters(params, fullNameTransformerParams); err != nil {
		return nil, err
	}

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
	return []transformers.SupportedDataType{
		transformers.StringDataType,
	}
}

func (t *FullNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncFullName
}
